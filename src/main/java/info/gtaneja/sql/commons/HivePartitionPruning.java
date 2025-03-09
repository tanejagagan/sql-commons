package info.gtaneja.sql.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.complex.ListVector;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionListWriter;
import org.apache.arrow.vector.complex.reader.VarCharReader;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.util.Text;
import org.duckdb.DuckDBConnection;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class HivePartitionPruning extends PartitionPruning {

    private static final String READ_TEXT_SQL = "SELECT size, filename, list_transform(parse_path(filename)[%s:%s], x -> split_part(x, '=', 2)) as partitions " +
            "FROM read_blob('%s')";
    private static final String PARTITION_SQL = "WITH A AS (SELECT * FROM %s)," +
            " B AS (SELECT size, filename, %s FROM A )" +
            " SELECT * FROM B where %s";

    private static final Field UNSCAPE_PARTITION_FIELD =
            new Field("unescaped_partitions", FieldType.notNullable(new ArrowType.List()),
                    List.of(new Field("children", FieldType.notNullable(new ArrowType.Utf8()), null)));

    protected static final MappedReader.Function UNESCAPE_FN = (sources, target) -> {
        ListVector resultVector = (ListVector) target;
        ListVector f = (ListVector) sources.get(0);
        UnionListReader reader = f.getReader();
        UnionListWriter writer = resultVector.getWriter();
        for (int i = 0; i < f.getValueCount(); i++) {
            reader.setPosition(i);
            writer.startList();
            writer.setPosition(i);
            while (reader.next()) {
                VarCharReader reader1 = reader.reader();
                Text text = reader1.readText();
                String res = HivePartitionPruning.unescapePathName(text.toString());
                writer.writeVarChar(new Text(res));
            }
            writer.endList();
        }
    };

    protected static String getPartitionSql(String[][] dataTypes,
                                          String tempTableName,
                                          String filter) {
        StringBuilder stringBuilder = new StringBuilder();
        for (int i = 0; i < dataTypes.length; i++) {
            String[] ss = dataTypes[i];
            String cast = String.format("cast(unescaped_partitions[%s] as %s) as %s", i + 1, ss[1], ss[0]);
            stringBuilder.append(cast);
            stringBuilder.append(",");
        }
        stringBuilder.deleteCharAt(stringBuilder.length() - 1);
        return String.format(PARTITION_SQL, tempTableName, stringBuilder, filter);
    }

    public static void main(String[] args) throws SQLException, JsonProcessingException {
        String sql0 = "select * from t";
        String sql1 = "select * from t where c1 = 10 and p1 = 50";
        String sql2 = "select * from t where true and p1 = 50";
        String sql4 = "select * from t where c1 = 10 or p1 = 50";
        String sql5 = "select * from t where c1 = 10";
        for (String sql : List.of(sql0, sql1, sql2, sql4, sql5)) {
            try (Connection connection = ConnectionPool.getConnection()) {
                String transformed = doQueryTransformation(connection, sql, Set.of("p1"));
                System.out.printf("SQL : %s \n", sql);
                System.out.printf("Transformed SQL : %s", transformed);
            }
        }
    }

    private static String doQueryTransformation(Connection connection, String sql, Set<String> partitionColumns) throws SQLException, JsonProcessingException {
        JsonNode tree = Transformations.parseToTree(connection, sql);
        JsonNode newTree = Transformations.transform(tree, Transformations.IS_SELECT,
                Transformations.removeNonPartitionColumnsPredicatesInQuery(partitionColumns));
        return Transformations.parseToSql(connection, newTree);
    }

    /**
     * @param basePath path can be relative to absolute
     * @param filter filter which include partition as well as no partition columns. Function will remove the filters which are not applicable
     * @param partitionDataTypes in order to cast the value to specific type
     * @return list of files and size of those files
     * @throws SQLException
     * @throws IOException  This is slightly complicated because we do not have function in duckdb which unescape hive path and java based udf are still not supported
     *                      its divided into 3 steps
     *                      1. Run the query on duckdb to get filename, size, partitions from duckdb.
     *                      Schema of read data is <filename string, size bigint, partitions array<string>>
     *                      2. collect the data from step 1 and unescape the partitions in java context using UNESCAPE_FN.
     *                      Schema at the end <filename string, size bigint, partitions <array<string>, unescape_partitions<array<string>>
     *                      3. Serialize the data in step 2 as temp table and run the pruning sql
     *                      Final Sql looks something like `select size, filename, cast(unescape_partitions[1] as date) as dt, ....from temp table where dt = ?
     *                      4. Remove all the filter which do not have partition columns
     */
    public static List<FileNameAndSize> pruneFiles(String basePath,
                                                   String filter,
                                                   String[][] partitionDataTypes) throws SQLException, IOException {
        String firstSql = getQueryString(basePath, partitionDataTypes.length);
        String tempTableName = "temp_table";
        List<FileNameAndSize> result = new ArrayList<>();
        try (DuckDBConnection connection = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(connection, allocator, firstSql, 1000);
             Closeable ignored = ConnectionPool.createTempTable(connection, allocator, reader,
                     UNESCAPE_FN, List.of("partitions"), UNSCAPE_PARTITION_FIELD, tempTableName)) {
            String x = HivePartitionPruning.getPartitionSql(partitionDataTypes, tempTableName, filter);
            String transformed = doQueryTransformation(connection, x,
                    Arrays.stream(partitionDataTypes).map(ss -> ss[0]).collect(Collectors.toSet()));

            try (ArrowReader reader1 = ConnectionPool.getReader(connection, allocator, transformed, 100)) {
                while (reader1.loadNextBatch()) {
                    VectorSchemaRoot root = reader1.getVectorSchemaRoot();
                    VarCharVector filename = (VarCharVector) root.getVector("filename");
                    BigIntVector size = (BigIntVector) root.getVector("size");
                    for (int i = 0; i < root.getRowCount(); i++) {
                        result.add(new FileNameAndSize(new String(filename.get(i)), size.get(i)));
                    }
                }
            }
        }
        return result;
    }

    protected static String getQueryString(String basePath,
                                         int partitionsLen) {
        int partitionStart = getPartitionStart(basePath);
        int partitionEnd = partitionStart + partitionsLen - 1;
        String readBlobPath = getReadBlobPath(basePath, partitionsLen, "parquet");
        return String.format(READ_TEXT_SQL, partitionStart, partitionEnd, readBlobPath);
    }

    private static String getReadBlobPath(String basePath, int partitionsLen, String format) {
        return basePath +
                "/*".repeat(Math.max(0, partitionsLen)) +
                "/*." +
                format;
    }

    private static int getPartitionStart(String basePath) {
        return basePath.split("/").length + 1;
    }

    public static String unescapePathName(String path) {
        StringBuilder sb = new StringBuilder();
        var i = 0;

        while (i < path.length()) {
            var c = path.charAt(i);
            if (c == '%' && i + 2 < path.length()) {
                int code;
                try {
                    code = Integer.parseInt(path.substring(i + 1, i + 3), 16);
                } catch (Exception e) {
                    code = -1;
                }
                if (code >= 0) {
                    sb.append((char) code);
                    i += 3;
                } else {
                    sb.append(c);
                    i += 1;
                }
            } else {
                sb.append(c);
                i += 1;
            }
        }
        return sb.toString();
    }
}



package info.gtaneja.sql.commons;


import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import java.io.Closeable;
import java.io.IOException;
import java.sql.SQLException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HivePartitionPruningTest {

    String basePath = "example/hive_table";
    String[][] partition = {{"dt", "date"}, {"p", "string"}};

    @Test
    public void getQueryString() throws SQLException, IOException {
        String queryString = HivePartitionPruning.getQueryString(basePath, 2);
        String countSql = String.format("with t as (%s) " +
                "select count(*) from t", queryString);
        assertEquals(3, ConnectionPool.collectFirst(countSql, Long.class));
    }

    @Test
    public void testMapReader() throws SQLException, IOException {
        String sql = "select 1 as count";
        MappedReader.Function function = (sources, target) -> {
            IntVector resultVector = (IntVector) target;
            IntVector f = (IntVector) sources.get(0);
            for (int i = 0; i < f.getValueCount(); i++) {
                resultVector.set(i, f.get(i) + 1);
            }
        };
        Field f = new Field("count_p", FieldType.notNullable(new ArrowType.Int(32, true)), null);
        testMapReaderInternal(sql, function, List.of("count"), f, "asdf", "select * from asdf", "select 1 as count, 2 as count_p");
    }

    @Test
    public void testMapReaderArray() throws SQLException, IOException {
        String sql = "select 10 as size, 'abc' as filename, ['a%20x', 'b'] as partitions";
        Field child = new Field("children", FieldType.notNullable(new ArrowType.Utf8()), null);
        List<String> sourceCol = List.of("partitions");
        Field targetField = new Field("unescaped_partitions", FieldType.notNullable(new ArrowType.List()), List.of(child));
        String[][] partitions = {
                {"a", "string"},
                {"b", "string"}
        };
        String temptableName = "tt";
        String testSql = HivePartitionPruning.getPartitionSql(partitions, temptableName, "true");
        testMapReaderInternal(sql, HivePartitionPruning.UNESCAPE_FN, sourceCol, targetField, temptableName, testSql, "select 10 as size, 'abc' as filename, 'a x' as a, 'b' as b");
    }

    @Test
    public void testReader() throws SQLException, IOException {
        String sql = "select 1 as count";
        try (DuckDBConnection connection = ConnectionPool.getConnection();
             RootAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, 10)) {
            VectorSchemaRoot root = reader.getVectorSchemaRoot();
            while (reader.loadNextBatch()) {
                root.contentToTSVString();
            }
        }
    }

    @Test
    public void testPruneFile() throws SQLException, IOException {
        HivePartitionPruning.pruneFiles(basePath, "p = 'a b'", partition);
        HivePartitionPruning.pruneFiles(basePath, "dt = '2024-01-01'", partition);
    }

    private void testMapReaderInternal(String sql,
                                       MappedReader.Function function,
                                       List<String> sourceCol,
                                       Field targetField,
                                       String tempTableName,
                                       String testSql,
                                       String expectedSql) throws SQLException, IOException {
        try (DuckDBConnection connection = ConnectionPool.getConnection();
             RootAllocator allocator = new RootAllocator();
             ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, 10);
             Closeable ignored = ConnectionPool.createTempTable(connection, allocator, reader, function, sourceCol, targetField, tempTableName)) {
            // the issue with temp tables for now are that they can be only be queries once since it reads the reader once.
            // It does not work even for queries such as `select * from a union select * from a` because it will require reader to be read twice.
            // Therefor we need to store the data of reader into materialized view so that it can be used multiple time
            String materializedTable = tempTableName + "_mat";
            ConnectionPool.execute(connection, String.format("CREATE TABLE %s AS SELECT * FROM %s", materializedTable, tempTableName));
            DuckDBTestUtil.isEqual(connection, allocator, testSql.replaceAll(tempTableName, materializedTable), expectedSql);
        }
    }
}


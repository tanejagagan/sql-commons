package info.gtaneja.sql.commons;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

public class DuckDBTestUtil {
    private static String IS_EQUAL = "WITH E AS (%s), " +
            " R AS (%s)," +
            " C AS (SELECT * FROM E EXCEPT SELECT * FROM R), " +
            " D AS (SELECT * FROM R EXCEPT SELECT * FROM E) " +
            " SELECT * FROM (SELECT 'L->' as s, * FROM C UNION SELECT 'R->' as s, * FROM D) ORDER BY s";
    ;

    public static void isEqual(String expected, String result) throws SQLException, IOException {
        String sql = String.format(IS_EQUAL, expected, result);
        try (DuckDBConnection connection = ConnectionPool.getConnection();
             BufferAllocator allocator = new RootAllocator();) {
            isEqual(connection, allocator, expected, result);
        }
    }

    public static void isEqual(DuckDBConnection connection, BufferAllocator allocator,
                               String expected, String result) throws SQLException, IOException {
        String sql = String.format(IS_EQUAL, expected, result);

        try(ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, 100)){
            StringBuilder stringBuilder = new StringBuilder();
            boolean failed = false;
            while (reader.loadNextBatch()) {
                failed = true;
                stringBuilder.append(reader.getVectorSchemaRoot().contentToTSVString());
            }
            if (failed) {
                throw new AssertionError(stringBuilder.toString());
            }
        }
}

        @Test
        public void testIsEqual() throws SQLException, IOException {
            isEqual("select 1", "select 1");
        }
    }

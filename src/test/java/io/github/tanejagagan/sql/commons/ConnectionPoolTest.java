package io.github.tanejagagan.sql.commons;

import io.github.tanejagagan.sql.commons.util.TestUtils;
import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

public class ConnectionPoolTest {

    @Test
    public void testPreConnectionSql() throws SQLException, IOException, InterruptedException {
        String tempLocation = newTempDir();
        try (Connection c = ConnectionPool.getConnection()) {
            ConnectionPool.execute(c, String.format("ATTACH '%s/file1.db' AS db1", tempLocation));
            ConnectionPool.execute(c, String.format("ATTACH '%s/file2.db' AS db2", tempLocation));
            ConnectionPool.execute(c, "use db1");
            ConnectionPool.execute(c, "create table t(id int)");
        }
        try {
            // This should fail
            test();
            throw new AssertionError("it should have failed");
        } catch (RuntimeException e) {
            // ignore the exception
        }
        ConnectionPool.addPreGetConnectionStatement("use db1");
        Thread.sleep(10);
        // This should pass now
        test();
        ConnectionPool.removePreGetConnectionStatement("use db1");
        try {
            // This should fail again
            test();
            throw new AssertionError("it should have failed");
        } catch (RuntimeException e) {
            // ignore the exception
        }
    }

    private void test() {
        try {
            TestUtils.isEqual("select 't' as name", "select * from (show tables)");
        } catch (SQLException | IOException | AssertionError e) {
            throw new RuntimeException(e);
        }
    }

    private static String newTempDir() throws IOException {
        Path tempDir = Files.createTempDirectory("duckdb-sql-commons-");
        return tempDir.toString();
    }

    @Test
    public void testArrowReader() throws SQLException, IOException {
        try(DuckDBConnection connection = ConnectionPool.getConnection();
            BufferAllocator allocator = new RootAllocator();
        ArrowReader reader = ConnectionPool.getReader(connection, allocator, "select * from generate_series(10)", 100)) {
            while (reader.loadNextBatch()){
                System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
            }
        }
    }

    @Test
    public void testBulkIngestionWithPartition() throws IOException, SQLException {
        String tempDir = newTempDir();
        String sql = "select generate_series, generate_series a from generate_series(10)";
        try(DuckDBConnection connection = ConnectionPool.getConnection();
            BufferAllocator allocator = new RootAllocator();
            ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, 1000)){
            ConnectionPool.bulkIngestToFile(reader, allocator, tempDir + "/bulk", List.of("a"), "parquet");
        }
    }

    @Test
    public void testBulkIngestionNoPartition() throws IOException, SQLException {
        String tempDir = newTempDir();
        String sql = "select generate_series, generate_series a from generate_series(10)";
        try(DuckDBConnection connection = ConnectionPool.getConnection();
            BufferAllocator allocator = new RootAllocator();
            ArrowReader reader = ConnectionPool.getReader(connection, allocator, sql, 1000)){
            ConnectionPool.bulkIngestToFile(reader, allocator, tempDir + "/bulk", List.of(), "parquet");
        }
    }
}

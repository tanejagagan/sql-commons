package io.github.tanejagagan.sql.commons;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

public class ConnectionPoolTest {

    @Test
    public void testPreConnectionSql() throws SQLException, IOException, InterruptedException {
        String tempLocation = "/tmp/test";
        try (Connection c = ConnectionPool.getConnection()) {
            ConnectionPool.execute(c, String.format("ATTACH '%s/file1.db' AS db1", tempLocation));
            ConnectionPool.execute(c, String.format("ATTACH '%s/file2.db' AS db2", tempLocation));
            ConnectionPool.execute(c, "use db1");
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
            DuckDBTestUtil.isEqual("select 't' as name", "select * from (show tables)");
        } catch (SQLException | IOException | AssertionError e) {
            throw new RuntimeException(e);
        }
    }
}

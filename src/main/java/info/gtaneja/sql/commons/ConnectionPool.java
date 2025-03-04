package info.gtaneja.sql.commons;

import org.duckdb.DuckDBConnection;

import java.sql.*;

public enum ConnectionPool {
    INSTANCE;

    DuckDBConnection connection = null;

    ConnectionPool() {
        try {
            this.connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> T collectFirst(String sql, Class<T> tClass) throws SQLException {
        try (Connection connection = getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
            try (ResultSet resultSet = statement.getResultSet()) {
                resultSet.next();
                return resultSet.getObject(1, tClass);
            }
        }
    }

    public static DuckDBConnection getConnection() throws SQLException {
        return INSTANCE.getConnectionInternal();
    }

    public DuckDBConnection getConnectionInternal() throws SQLException {
        return (DuckDBConnection) connection.duplicate();
    }
}

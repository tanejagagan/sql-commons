package io.github.tanejagagan.sql.commons;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBResultSet;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public enum ConnectionPool {
    INSTANCE;

    private final DuckDBConnection connection;

    private final ArrayList<String> preGetConnectionStatements = new ArrayList<>();

    ConnectionPool() {
        try {
            this.connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * @param connection
     * @param sql sql to be executed
     * @param tClass class of the return object
     * @return fist value of the result set
     * @param <T>
     * @throws SQLException
     */
    public static <T> T collectFirst(Connection connection, String sql, Class<T> tClass) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            statement.execute(sql);
            try (ResultSet resultSet = statement.getResultSet()) {
                resultSet.next();
                return resultSet.getObject(1, tClass);
            }
        }
    }

    /**
     *
     * @param sql
     * @param tClass
     * @return
     * @param <T>
     * @throws SQLException
     */
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

    /**
     *
     * @param sql
     * @throws SQLException
     * @throws IOException
     * Used for debugging to print the output of the sql
     */
    public static void printResult(String sql) throws SQLException, IOException {
        try (Connection connection = getConnection();
             BufferAllocator rootAllocator = new RootAllocator();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
            try (DuckDBResultSet resultSet = (DuckDBResultSet) statement.getResultSet();
                 ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(rootAllocator, 1000)) {
                while (reader.loadNextBatch()){
                    System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
                }
            }
        }
    }

    /**
     *
     * @param connection
     * @param allocator
     * @param sql
     * @throws SQLException
     * @throws IOException
     * Used for debugging to print the output of the sql
     */
    public static void printResult(Connection connection, BufferAllocator allocator, String sql) throws SQLException, IOException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            try (DuckDBResultSet resultSet = (DuckDBResultSet) statement.getResultSet();
                 ArrowReader reader = (ArrowReader) resultSet.arrowExportStream(allocator, 1000)) {
                while (reader.loadNextBatch()){
                    System.out.println(reader.getVectorSchemaRoot().contentToTSVString());
                }
            }
        }
    }

    /**
     *
     * @param connection
     * @param allocator
     * @param reader
     * @param function
     * @param sourceColumns
     * @param targetField
     * @param tableName
     * @return
     * @throws IOException
     *
     */
    public static Closeable createTempTable(DuckDBConnection connection,
                                            BufferAllocator allocator,
                                            ArrowReader reader,
                                            MappedReader.Function function,
                                            List<String> sourceColumns,
                                            Field targetField,
                                            String tableName) throws IOException {
        ArrowReader mappedReader = new MappedReader(allocator, reader, function, sourceColumns,
                targetField);
        final ArrowArrayStream arrow_array_stream = ArrowArrayStream.allocateNew(allocator);
        Data.exportArrayStream(allocator, mappedReader, arrow_array_stream);
        connection.registerArrowStream(tableName, arrow_array_stream);
        return new Closeable() {
            @Override
            public void close() throws IOException {
                mappedReader.close();
                arrow_array_stream.close();
            }
        };
    }

    /**
     *
     * @param connection
     * @param sql
     * @throws SQLException
     */
    public static void execute(Connection connection, String sql) throws SQLException {
        try(Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    /**
     *
     * @param connection
     * @param allocator
     * @param sql
     * @param batchSize
     * @return
     * @throws SQLException
     */
    public static ArrowReader getReader(DuckDBConnection connection,
                                        BufferAllocator allocator,  String sql, int batchSize) throws SQLException {
        final Statement statement = connection.createStatement();
        statement.execute(sql);
        return new ArrowReader(allocator) {
            final Statement _statement = statement;
            final DuckDBResultSet resultSet = (DuckDBResultSet) statement.getResultSet();
            private final ArrowReader internal = (ArrowReader) resultSet.arrowExportStream(allocator, batchSize);
            @Override
            public boolean loadNextBatch() throws IOException {
                return internal.loadNextBatch();
            }

            @Override
            public long bytesRead() {
                return internal.bytesRead();
            }

            @Override
            protected void closeReadSource() throws IOException {
                internal.close();
                try {
                    resultSet.close();
                    _statement.close();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }

            @Override
            protected Schema readSchema() throws IOException {
                return internal.getVectorSchemaRoot().getSchema();
            }

            @Override
            public VectorSchemaRoot getVectorSchemaRoot() throws IOException {
                return internal.getVectorSchemaRoot();
            }
        };
    }

    public static DuckDBConnection getConnection() throws SQLException {
        return INSTANCE.getConnectionInternal();
    }

    public DuckDBConnection getConnectionInternal() throws SQLException {
        DuckDBConnection result = (DuckDBConnection) connection.duplicate();
        try (Statement statement = result.createStatement()) {
            for (String sql : preGetConnectionStatements) {
                statement.execute(sql);
            }
        }
        return result;
    }

    /**
     *
     * @param sql add a sql which will be executed before returning the connection by the method getConnection()
     *            This method should be invoked at the beginning of the main function.
     *            Typical use case will be set specific catalog/schema.
     */
    public static void addPreGetConnectionStatement(String sql) {
        INSTANCE.preGetConnectionStatements.add(sql);
    }

    /**
     *
     * @param sql removes a sql which are to be executed when getConnection() is invoked.
     */
    public static void removePreGetConnectionStatement(String sql) {
        INSTANCE.preGetConnectionStatements.remove(sql);
    }
}

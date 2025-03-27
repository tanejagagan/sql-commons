package io.github.tanejagagan.sql.commons;

import org.apache.arrow.c.ArrowArrayStream;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.util.AutoCloseables;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.duckdb.DuckDBConnection;
import org.duckdb.DuckDBDriver;
import org.duckdb.DuckDBResultSet;

import java.io.Closeable;
import java.io.IOException;
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

public enum ConnectionPool {
    INSTANCE;

    private final DuckDBConnection connection;

    private final ArrayList<String> preGetConnectionStatements = new ArrayList<>();

    ConnectionPool() {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            Properties props = new Properties();
            props.setProperty(DuckDBDriver.JDBC_STREAM_RESULTS, String.valueOf(true));
            this.connection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:", props);
        } catch (SQLException | ClassNotFoundException e) {
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
     */
    public static <T> T collectFirst(Connection connection, String sql, Class<T> tClass) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            try (ResultSet resultSet = statement.getResultSet()) {
                resultSet.next();
                return resultSet.getObject(1, tClass);
            }
            catch (SQLException e ){
                throw new RuntimeException("Error collecting result set for sql " + sql, e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("Error running sql" + sql, e);
        }
    }

    public static <T> Iterable<T> collectFirstColumn(Connection connection, String sql, Class<T> tClass) {
        return collectAll(connection, sql, rs -> rs.getObject(1, tClass), tClass);
    }

    public static <T> Iterable<T> collectAll(Connection connection, String sql, Extractor<T> extractor, Class<T> tClass) {
        List<T> result = new ArrayList<>();
        try (Statement statement = connection.createStatement()) {
            statement.execute(sql);
            try (ResultSet resultSet = statement.getResultSet()) {
                while (resultSet.next()) {
                    result.add(extractor.apply(resultSet));
                }
            }
            catch (SQLException e ){
                throw new RuntimeException("Error collecting result set for sql " + sql, e);
            }
            return result;
        } catch (SQLException e) {
            throw new RuntimeException("Error running sql" + sql, e);
        }
    }

    /**
     *
     * @param sql
     * @param tClass
     * @return
     * @param <T>
     */
    public static <T> T collectFirst(String sql, Class<T> tClass) throws SQLException {
        try (DuckDBConnection connection = getConnection()) {
            return collectFirst(connection, sql, tClass);
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
        ArrowReader mappedReader = new MappedReader(allocator.newChildAllocator("mmmm", 0, Long.MAX_VALUE), reader, function, sourceColumns,
                targetField);
        final ArrowArrayStream arrow_array_stream = ArrowArrayStream.allocateNew(allocator);
        Data.exportArrayStream(allocator, mappedReader, arrow_array_stream);
        connection.registerArrowStream(tableName, arrow_array_stream);
        return () -> {
            try {
                AutoCloseables.close(mappedReader, arrow_array_stream);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
    }

    /**
     *
     * @param connection
     * @param sql
     * @return
     */
    public static boolean execute(Connection connection, String sql)  {
        try(Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Error running sql :" + sql,  e);
        }
    }

    /**
     *
     * @param sql
     * @return
     */
    public static boolean execute(String sql)  {
        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            return statement.execute(sql);
        } catch (SQLException e) {
            throw new RuntimeException("Error running sql :" + sql,  e);
        }
    }

    /**
     *
     * @param sqls
     * @return
     */
    public static int[] executeBatch(String[] sqls) {
        try(Connection connection = ConnectionPool.getConnection();
            Statement statement = connection.createStatement()) {
            for(String sql : sqls) {
                statement.addBatch(sql);
            }
            return statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Error running sqls :",  e);
        }
    }

    public static int[] executeBatch(Connection connection, String[] sqls) {
        try(Statement statement = connection.createStatement()) {
            for(String sql : sqls) {
                statement.addBatch(sql);
            }
            return statement.executeBatch();
        } catch (SQLException e) {
            throw new RuntimeException("Error running sqls :",  e);
        }
    }

    /**
     *
     * @param connection
     * @param allocator
     * @param sql
     * @param batchSize
     * @return
     */
    public static ArrowReader getReader(DuckDBConnection connection,
                                        BufferAllocator allocator,
                                        String sql,
                                        int batchSize)  {
        try {
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
        } catch (SQLException e ) {
            throw new RuntimeException(e);
        }
    }

    public static DuckDBConnection getConnection()  {
        return INSTANCE.getConnectionInternal();
    }

    private DuckDBConnection getConnectionInternal() {
        try {
            DuckDBConnection result = (DuckDBConnection) connection.duplicate();
            Statement statement = result.createStatement();
            for (String sql : preGetConnectionStatements) {
                statement.execute(sql);
            }
            return result;
        } catch (SQLException e ){
            throw new RuntimeException("Error creating connection " , e);
        }
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

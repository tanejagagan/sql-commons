package io.github.tanejagagan.sql.commons.delta;

import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.tanejagagan.sql.commons.delta.DeltaLakePartitionPruning.*;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class DeltaLakePartitionPruningTest {
    static final String basePath = "example/delta_table";
    static final String[][] partition = {{"dt", "date"}, {"p", "string"}};

    private static void assertSize(int expectedSize, String basePath, String filter) throws IOException, SQLException {
        List<?> result = pruneFiles(basePath, filter, partition);
        assertEquals(expectedSize, result.size(), result.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    @Test
    public void pruneFilesNoPartitionTest() throws SQLException, IOException {
        assertSize(8, basePath, "k3=3");
        assertSize(8, basePath, "p=x");
        assertSize(8, basePath, "");
    }

    @Test
    public void pruneFilesPartitionTest() throws SQLException, IOException {
        assertSize(3, basePath, "dt=cast('2024-01-01' as date)");
        assertSize(1, basePath, "p='x'");
        assertSize(1, basePath, "dt=cast('2024-01-01' as date) and p = 'x'");
        assertSize(2, basePath,"p='a b' or p ='x'");
    }
}
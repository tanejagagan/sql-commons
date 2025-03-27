package io.github.tanejagagan.sql.commons;

import org.testng.annotations.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.stream.Collectors;

import static io.github.tanejagagan.sql.commons.DeltaLakePartitionPruning.pruneFiles;
import static org.junit.jupiter.api.Assertions.assertEquals;


public class DeltaLakePartitionPruningTest {
    static final String basePath = "example/delta_table";
    private static void assertSize(int expectedSize, String basePath, String filter) throws IOException {
        List<?> result = pruneFiles(basePath, filter);
        assertEquals(expectedSize, result.size(), result.stream().map(Object::toString).collect(Collectors.joining(",")));
    }

    @Test
    public void pruneFilesNoPartitionTest() throws SQLException, IOException {
        assertSize(8, basePath, "k3=3");
        assertSize(8, basePath, "");
    }

    @Test
    public void pruneFilesPartitionTest() throws SQLException, IOException {
        assertSize(1, basePath, "dt=2024-05-01");
        assertSize(1, basePath, "p=x");
        assertSize(1, basePath,"p = 'a b'");
    }
}
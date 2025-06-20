package io.github.tanejagagan.sql.commons.planner;

import io.github.tanejagagan.sql.commons.Transformations;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

public class SplitPlannerTest {


    @Test
    public void testSplitHive() throws SQLException, IOException {
        String[][] partitions = {
                {"dt", "string"},
                {"p", "string"}
        };
        var path = "example/hive_table";
        var sql = String.format("select * from read_parquet('%s')", path);
        var splits = SplitPlanner.getSplits(Transformations.parseToTree(sql), partitions, 1024 * 1024 * 1024);
        assertEquals(1, splits.size());
        assertEquals(3, splits.get(0).size());
    }

    @Test
    public void testSplitDelta() throws SQLException, IOException {
        String[][] partitions = {
                {"dt", "string"},
                {"p", "string"}
        };
        var path = "example/delta_table";
        var sql = String.format("select * from read_delta('%s')", path);
        var splits = SplitPlanner.getSplits(Transformations.parseToTree(sql),
                partitions, 1024 * 1024 * 1024);
        assertEquals(1, splits.size());
        assertEquals(8, splits.get(0).size());
    }
}

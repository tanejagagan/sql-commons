package io.github.tanejagagan.sql.commons.planner;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.tanejagagan.sql.commons.FileStatus;
import io.github.tanejagagan.sql.commons.Transformations;
import io.github.tanejagagan.sql.commons.hive.HivePartitionPruning;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;

public interface SplitPlanner {



    public static List<List<FileStatus>> getSplits(JsonNode tree,
                                                   long maxSplitSize) throws SQLException, IOException {
        var filterExpression = Transformations.getWhereClause(tree);
        var catalogSchemaAndTable = Transformations.getTableOrPath(tree, null, null);
        var tableFunction = Transformations.getTableFunction(tree);
        var path = catalogSchemaAndTable.tableOrPath();
        List<FileStatus> fileStatuses;
        switch (tableFunction) {
            case "read_parquet" -> {
                var partitionDataTypes  = Transformations.getHivePartition(tree);
                fileStatuses = HivePartitionPruning.pruneFiles(path,
                        tree, partitionDataTypes);
            }
            case "read_delta" ->
                    fileStatuses = io.github.tanejagagan.sql.commons.delta.PartitionPruning.pruneFiles(path, filterExpression);
            default -> throw new SQLException("unsupported type : " + tableFunction);
        }

        fileStatuses.sort(Comparator.comparing(FileStatus::lastModified));
        return getSplits(maxSplitSize, fileStatuses);
    }

    private static ArrayList<List<FileStatus>> getSplits(long maxSplitSize, List<FileStatus> fileStatuses) {
        var result = new ArrayList<List<FileStatus>>();
        var current = new ArrayList<FileStatus>();
        long currentSize = 0;
        for (FileStatus fileStatus : fileStatuses) {
            current.add(fileStatus);
            currentSize += fileStatus.size();
            if(currentSize > maxSplitSize) {
                result.add(current);
                current = new ArrayList<>();
                currentSize = 0;
            }
        }
        if(!current.isEmpty()) {
            result.add(current);
        }
        return result;
    }
}

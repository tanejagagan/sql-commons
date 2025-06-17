package io.github.tanejagagan.sql.commons.planner;

import com.fasterxml.jackson.databind.JsonNode;
import io.github.tanejagagan.sql.commons.FileStatus;
import io.github.tanejagagan.sql.commons.Transformations;
import io.github.tanejagagan.sql.commons.hive.HivePartitionPruning;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public interface SplitPlanner {

    public static List<List<FileStatus>> getSplits(String path,
                                                   String filter,
                                                   String[][] partitionDataTypes,
                                                   String type,
                                                   long maxSplitSize) throws SQLException, IOException {
        JsonNode filterExpression = Transformations.getWhereClause(Transformations.parseToTree("select * from t where" + filter));
        List<FileStatus> fileStatuses;
        switch (type) {
            case "hive" -> fileStatuses = HivePartitionPruning.pruneFiles(path,
                    filter, partitionDataTypes);
            case "delta" ->
                    fileStatuses = io.github.tanejagagan.sql.commons.delta.PartitionPruning.pruneFiles(path, filterExpression);
            default -> throw new SQLException("unsupported type : " + type);
        }
        fileStatuses.sort(Comparator.comparing(FileStatus::lastModified));
        var result = new ArrayList<List<FileStatus>>();
        var current = new ArrayList<FileStatus>();
        long currentSize = 0;
        for (FileStatus fileStatus : fileStatuses) {
            if (currentSize < maxSplitSize) {
                current.add(fileStatus);
                currentSize += fileStatus.size();
            } else {
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

package io.github.tanejagagan.sql.commons;

import io.delta.kernel.Scan;
import io.delta.kernel.Snapshot;
import io.delta.kernel.Table;
import io.delta.kernel.data.FilteredColumnarBatch;
import io.delta.kernel.data.Row;
import io.delta.kernel.defaults.engine.DefaultEngine;
import io.delta.kernel.engine.Engine;
import io.delta.kernel.exceptions.TableNotFoundException;
import io.delta.kernel.expressions.*;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.types.*;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

/**
 * Utility class for efficient file pruning in Delta tables using partition filters.
 * Provides mechanisms to filter Delta table files based on their partition values.
 */
public class DeltaLakePartitionPruning {
    private static final Logger logger = LoggerFactory.getLogger(DeltaLakePartitionPruning.class);

    // Common date and time formatters - create once for reuse
    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ISO_DATE;
    private static final DateTimeFormatter US_DATE = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter US_DATETIME_FORMAT = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");

    // Reusable UTC zone for timestamp conversions
    private static final ZoneId UTC = ZoneId.of("UTC");

    /**
     * Parses a single filter expression of format "key = value" into a key-value pair.
     * Key is converted to uppercase for case-insensitive matching.
     *
     * @param filterExpression the filter expression to parse (e.g., "column = 'value'")
     * @return Map containing the extracted key and value, or empty map if expression is invalid
     */
    protected static Map<String, String> parseFilters(String filterExpression) {
        Map<String, String> filterMap = new HashMap<>();

        if (filterExpression == null || filterExpression.isEmpty()) {
            return filterMap;
        }

        if (filterExpression.contains("=")) {
            String[] parts = filterExpression.split("=", 2);
            if (parts.length == 2) {
                String key = parts[0].trim().toUpperCase();
                String value = parts[1].trim();

                // Remove surrounding quotes if present
                if ((value.startsWith("'") && value.endsWith("'")) ||
                        (value.startsWith("\"") && value.endsWith("\""))) {
                    value = value.substring(1, value.length() - 1);
                }

                // Add to map if key is not empty
                if (!key.isEmpty()) {
                    filterMap.put(key, value);
                }
            }
        }
        return filterMap;
    }

    /**
     * Creates a Literal expression from a string value based on the target data type.
     *
     * @param value The string value to convert
     * @param dataType The target DataType to convert to
     * @return An Expression representing the literal value with the appropriate type
     * @throws IllegalArgumentException if the value cannot be converted to the specified type
     */
    private static Expression createLiteralFromString(String value, DataType dataType) {
        if (value == null) {
            return Literal.ofNull(dataType);
        }

        try {
            // String type handling - most common case first for efficiency
            if (dataType instanceof StringType) {
                return Literal.ofString(value);
            }

            // Boolean handling with basic validation
            if (dataType instanceof BooleanType) {
                return handleBooleanLiteral(value);
            }

            // Date and timestamp handling
            if (dataType instanceof DateType) {
                return handleDateLiteral(value);
            }

            if (dataType instanceof TimestampType) {
                return handleTimestampLiteral(value);
            }

            // Default case for unsupported types
            logger.warn("Unsupported data type: {}. Using string representation.", dataType);
            return Literal.ofString(value);

        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to convert value '" + value + "' to type " + dataType, e);
        }
    }

    /**
     * Handles conversion of string to boolean literal
     */
    private static Expression handleBooleanLiteral(String value) {
        if (value.equalsIgnoreCase("true") || value.equals("1")) {
            return Literal.ofBoolean(true);
        } else if (value.equalsIgnoreCase("false") || value.equals("0")) {
            return Literal.ofBoolean(false);
        } else {
            throw new IllegalArgumentException("Cannot convert '" + value + "' to boolean");
        }
    }


    /**
     * Handles conversion of string to date literal
     */
    private static Expression handleDateLiteral(String value) {
        // Fast path for epoch days
        try {
            return Literal.ofDate(Integer.parseInt(value));
        } catch (NumberFormatException e) {
            // Not a number, try as date string
        }

        // Try ISO format first (most common)
        try {
            LocalDate date = LocalDate.parse(value, ISO_DATE);
            return Literal.ofDate((int) date.toEpochDay());
        } catch (DateTimeParseException e) {
            // Try US format (second most common)
            try {
                LocalDate date = LocalDate.parse(value, US_DATE);
                return Literal.ofDate((int) date.toEpochDay());
            } catch (DateTimeParseException e2) {
                throw new IllegalArgumentException(
                        "Cannot convert '" + value + "' to date. Expected format: yyyy-MM-dd or MM/dd/yyyy");
            }
        }
    }

    /**
     * Handles conversion of string to timestamp literal
     */
    private static Expression handleTimestampLiteral(String value) {
        // Fast path for epoch millis
        try {
            return Literal.ofTimestamp(Long.parseLong(value));
        } catch (NumberFormatException e) {
            // Not a number, try as timestamp string
        }

        // Try ISO timestamp format
        try {
            Instant instant = Instant.parse(value);
            return Literal.ofTimestamp(instant.toEpochMilli());
        } catch (DateTimeParseException e) {
            // Try standard datetime format
            try {
                LocalDateTime localDateTime = LocalDateTime.parse(value, DATETIME_FORMAT);
                Instant instant = localDateTime.atZone(UTC).toInstant();
                return Literal.ofTimestamp(instant.toEpochMilli());
            } catch (DateTimeParseException e2) {
                // Try US datetime format
                try {
                    LocalDateTime localDateTime = LocalDateTime.parse(value, US_DATETIME_FORMAT);
                    Instant instant = localDateTime.atZone(UTC).toInstant();
                    return Literal.ofTimestamp(instant.toEpochMilli());
                } catch (DateTimeParseException e3) {
                    throw new IllegalArgumentException(
                            "Cannot parse timestamp: " + value + ". Supported formats: ISO-8601, yyyy-MM-dd HH:mm:ss, MM/dd/yyyy HH:mm:ss");
                }
            }
        }
    }

    /**
     * Builds a compound predicate by combining individual predicates with AND operations.
     *
     * @param predicates List of predicates to combine
     * @return The combined predicate, or null if no predicates exist
     */
    private static Predicate buildCompoundPredicate(List<Predicate> predicates) {
        if (predicates.isEmpty()) {
            return null;
        }

        if (predicates.size() == 1) {
            return predicates.get(0);
        }

        // For multiple predicates, fold them into a compound AND predicate
        return predicates.stream()
                .reduce((left, right) -> new Predicate("and", left, right))
                .orElse(null);
    }

    /**
     * Prunes and retrieves files from a Delta table based on partition filter criteria.
     *
     * This method queries a Delta table at the specified path and applies partition-based
     * filters to return only those files that match the filter criteria. The filter string
     * is parsed into key-value pairs, and only partition columns are considered for filtering.
     *
     * @param basePath The file system path to the Delta table
     * @param filter A string representing filter criteria in format "key1=value1,key2=value2"
     *               where keys are column names and values are the filter values
     * @return A list of FileNameAndSize objects representing the filtered Delta table files
     * @throws IOException If an I/O error occurs while accessing the Delta table
     */
    public static List<FileNameAndSize> pruneFiles(String basePath, String filter) throws IOException {
        List<FileNameAndSize> result = new ArrayList<>();

        if (basePath == null || basePath.isEmpty()) {
            throw new IllegalArgumentException("Base path cannot be null or empty");
        }

        Map<String, String> parseFilters = parseFilters(filter);

        try {
            // Initialize the Delta engine
            Engine engine = DefaultEngine.create(new Configuration());
            Table deltaTable = Table.forPath(engine, basePath);

            // Get latest snapshot once for efficiency
            Snapshot snapshot = deltaTable.getLatestSnapshot(engine);

            // Collect partition column metadata
            List<String> partitionColumnNames = snapshot.getPartitionColumnNames(engine);

            // If no partition columns, return all files (no pruning possible)
            if (partitionColumnNames.isEmpty()) {
                return getAllTableFiles(engine, snapshot);
            }

            // Get schema for data type information
            StructType schema = snapshot.getSchema(engine);

            // Create case-insensitive mapping of partition columns to their data types
            Map<String, DataType> uppercasePartitionColumnTypes = buildPartitionColumnMap(
                    partitionColumnNames, schema);

            // Create predicates for each valid partition filter
            List<Predicate> partitionFilters = createPartitionFilters(
                    parseFilters, uppercasePartitionColumnTypes);

            // Build the scan with filters if applicable
            Scan scan = buildScanWithFilters(engine, snapshot, partitionFilters);

            // Process the scan to collect matching files
            result = collectMatchingFiles(engine, scan);

        } catch (TableNotFoundException e) {
            logger.warn("Delta table not found at path: {}", basePath);
        } catch (Exception e) {
            logger.error("Error processing Delta table files from {}: {}", basePath, e.getMessage(), e);
            throw new IOException("Failed to process Delta table files", e);
        }

        return result;
    }

    /**
     * Builds a mapping of uppercase partition column names to their data types.
     */
    private static Map<String, DataType> buildPartitionColumnMap(
            List<String> partitionColumnNames, StructType schema) {

        Map<String, DataType> uppercasePartitionColumnTypes = new HashMap<>();
        for (String columnName : partitionColumnNames) {
            DataType dataType = schema.get(columnName).getDataType();
            uppercasePartitionColumnTypes.put(columnName.toUpperCase(), dataType);
        }
        return uppercasePartitionColumnTypes;
    }

    /**
     * Creates partition filter predicates from parsed filter expressions.
     */
    private static List<Predicate> createPartitionFilters(
            Map<String, String> parseFilters,
            Map<String, DataType> uppercasePartitionColumnTypes) {

        List<Predicate> partitionFilters = new ArrayList<>();

        for (Map.Entry<String, String> entry : parseFilters.entrySet()) {
            String key = entry.getKey();
            String value = entry.getValue();

            // Only process if this is a partition column
            if (uppercasePartitionColumnTypes.containsKey(key)) {
                DataType dataType = uppercasePartitionColumnTypes.get(key);

                try {
                    // Convert string value to appropriate literal
                    Expression literalValue = createLiteralFromString(value, dataType);

                    // Create equality predicate
                    Predicate singleFilter = new Predicate(
                            "=",
                            new Column(key),
                            literalValue
                    );
                    partitionFilters.add(singleFilter);
                } catch (IllegalArgumentException e) {
                    logger.error("Error in filter for column '{}': {}", key, e.getMessage());
                    // Continue with other filters
                }
            }
        }

        return partitionFilters;
    }

    /**
     * Builds a scan with the specified filters.
     */
    private static Scan buildScanWithFilters(
            Engine engine, Snapshot snapshot, List<Predicate> partitionFilters) {

        Predicate finalFilter = buildCompoundPredicate(partitionFilters);

        if (finalFilter != null) {
            return snapshot.getScanBuilder(engine)
                    .withFilter(engine, finalFilter)
                    .build();
        } else {
            return snapshot.getScanBuilder(engine).build();
        }
    }

    /**
     * Collects all files from a Delta table without filtering.
     */
    private static List<FileNameAndSize> getAllTableFiles(Engine engine, Snapshot snapshot) {
        List<FileNameAndSize> result = new ArrayList<>();
        Scan scan = snapshot.getScanBuilder(engine).build();
        return collectMatchingFiles(engine, scan);
    }

    /**
     * Processes a scan to collect matching files.
     */
    private static List<FileNameAndSize> collectMatchingFiles(Engine engine, Scan scan) {
        List<FileNameAndSize> result = new ArrayList<>();

        try (CloseableIterator<FilteredColumnarBatch> fileIter = scan.getScanFiles(engine)) {
            while (fileIter.hasNext()) {
                FilteredColumnarBatch batch = fileIter.next();
                try (CloseableIterator<Row> rowIter = batch.getRows()) {
                    while (rowIter.hasNext()) {
                        Row row = rowIter.next();
                        FileStatus fileStatus = InternalScanFileUtils.getAddFileStatus(row);
                        result.add(new FileNameAndSize(fileStatus.getPath(), fileStatus.getSize()));
                    }
                }
            }
        } catch (IOException e) {
            logger.error("Error processing scan files: {}", e.getMessage());
        }

        return result;
    }
}
package io.github.tanejagagan.sql.commons.delta;

import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.expressions.*;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.time.*;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.Arrays;

import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.function.Function;


import static io.github.tanejagagan.sql.commons.delta.DataType.getCastRules;


public class Transformations {

    // TODO
    // 1. Binary Type
    // 2. Decimal Type
    // 3. ArrayType, MapType, StructType, StructField
    // https://github.com/apache/spark/blob/87b9866903baa3b291058b3613f9954ec62c178c/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Cast.scala#L4
    private static final Map<String, Function<Expression, Object>> parsingFunctions = Map.ofEntries(
            Map.entry(DataType.TINY_INT, expr -> Byte.parseByte(expr.toString())),
            Map.entry(DataType.SMALL_INT, expr -> Short.parseShort(expr.toString())),
            Map.entry(DataType.INT, expr -> Integer.parseInt(expr.toString())),
            Map.entry(DataType.BIG_INT, expr -> Long.parseLong(expr.toString())),
            Map.entry(DataType.FLOAT, expr -> Float.parseFloat(expr.toString())),
            Map.entry(DataType.DOUBLE, expr -> Double.parseDouble(expr.toString())),
            Map.entry(DataType.DECIMAL, expr -> new java.math.BigDecimal(expr.toString())),
            Map.entry(DataType.STRING, expr -> expr.toString()),
            Map.entry(DataType.BINARY, expr -> {
                // Assuming Expression stores a byte[] as its value
                if (expr instanceof Literal) {
                    return ((Literal) expr).getDataType(); // Replace this with the actual method
                }
                throw new IllegalArgumentException("Invalid expression type for BINARY: " + expr);
            }),
            Map.entry(DataType.BOOLEAN, expr -> Boolean.parseBoolean(expr.toString())),
            Map.entry(DataType.TIMESTAMP, expr -> {
                return java.sql.Timestamp.valueOf(expr.toString()).getTime() * 1000; // Convert to microseconds
            }),
            Map.entry(DataType.TIMESTAMP_NTZ, expr -> java.time.LocalDateTime.parse(expr.toString())),
            Map.entry(DataType.DATE, expr -> Date.parse(expr.toString()))

    );

    private static final Map<String, Function<Object, Expression>> castingFunctions = Map.ofEntries(
            Map.entry(DataType.TINY_INT, DataTypeConverter::toTinyInt),
            Map.entry(DataType.SMALL_INT, DataTypeConverter::toSmallInt),
            Map.entry(DataType.INT, DataTypeConverter::toInt),
            Map.entry(DataType.BIG_INT, DataTypeConverter::toBigInt),
            Map.entry(DataType.FLOAT, DataTypeConverter::toFloat),
            Map.entry(DataType.DOUBLE, DataTypeConverter::toDouble),
            Map.entry(DataType.STRING, DataTypeConverter::toString),
            Map.entry(DataType.BINARY, DataTypeConverter::toBinary),
            Map.entry(DataType.BOOLEAN, DataTypeConverter::toBoolean),
            Map.entry(DataType.TIMESTAMP, DataTypeConverter::toTimestamp),
            Map.entry(DataType.TIMESTAMP_NTZ, DataTypeConverter::toTimestamp),
            Map.entry(DataType.DATE, DataTypeConverter::toDate),

            // TODO Add Decimal support
            Map.entry(DataType.DECIMAL, value -> {
                // Default precision and scale
                int defaultPrecision = 10;
                int defaultScale = 0;
                // Call the toDecimal method with default precision and scale
                return DataTypeConverter.toDecimal(value, defaultPrecision, defaultScale);
            })

    );

    /**
     * Perform a two-step cast: parse the source type and then cast to the target type.
     *
     * @param expr       The expression to cast.
     * @param sourceType The source type of the expression (e.g., TINYINT).
     * @param targetType The target type to cast to (e.g., INT).
     * @return The casted expression.
     */
    private static Expression castExpression(Expression expr, String sourceType, String targetType) {
        // Step 1: Parse the source type
        Function<Expression, Object> parseFunction = parsingFunctions.get(sourceType);
        if (parseFunction == null) {
            throw new UnsupportedOperationException("Unsupported source type: " + sourceType);
        }
        Object intermediateValue = parseFunction.apply(expr);

        // Step 2: Cast to the target type
        Function<Object, Expression> castFunction = castingFunctions.get(targetType);
        if (castFunction == null) {
            throw new UnsupportedOperationException("Unsupported target type: " + targetType);
        }
        return castFunction.apply(intermediateValue);
    }


    // Function to check if a type can be cast to another type
    private static boolean canCast(String fromType, String toType) {
        return getCastRules().get(fromType).contains(toType);
    }

    /**
     * Converts a string timestamp to microseconds since UTC epoch.
     *
     * @param value The timestamp string value
     * @param zoneId The time zone to use when timestamp has no zone
     *        If null, treats timestamp as UTC
     * @return microseconds since UTC epoch, or null if conversion failed
     */
    public static Long timestampToMicrosSinceEpochUTC(String value, ZoneId zoneId) {
        if (value == null || value.isEmpty()) {
            return null;
        }

        // Default to UTC if zoneId is null
        ZoneId effectiveZoneId = (zoneId != null) ? zoneId : ZoneOffset.UTC;

        // Try parsing as a number first
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException e) {
            // Not a number, continue to date parsing
        }

        try {
            // First check if the string contains timezone information (Z, +, -)
            if (value.endsWith("Z") || value.contains("+") ||
                    (value.contains("-") && value.lastIndexOf("-") > value.indexOf("-") + 2)) {

                // Parse as a timestamp with timezone
                Instant instant;
                try {
                    if (value.contains("T")) {
                        // ISO-8601 format
                        instant = Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(value));
                    } else {
                        // Try with custom formatter
                        DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                                .appendPattern("yyyy-MM-dd HH:mm:ss")
                                .optionalStart()
                                .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                                .optionalEnd()
                                .appendOffset("+HH:MM", "Z")
                                .toFormatter();
                        instant = Instant.from(formatter.parse(value));
                    }
                } catch (Exception e) {
                    // Try with OffsetDateTime as a fallback
                    instant = OffsetDateTime.parse(value).toInstant();
                }

                return instant.toEpochMilli() * 1000;

            } else {
                // Parse as a LocalDateTime (no timezone info)
                LocalDateTime localDateTime;

                if (value.contains("T")) {
                    // ISO-8601-like format without timezone
                    localDateTime = LocalDateTime.parse(value);
                } else if (value.contains("-") && value.contains(":")) {
                    // "YYYY-MM-DD HH:MM:SS[.fffffffff]" format
                    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss[.SSSSSSSSS]");
                    localDateTime = LocalDateTime.parse(value, formatter);
                } else if (value.contains("-") && !value.contains(":")) {
                    // Date only format (yyyy-MM-dd)
                    localDateTime = LocalDate.parse(value).atStartOfDay();
                } else {
                    // Try with flexible formatter as last resort
                    DateTimeFormatter formatter = new DateTimeFormatterBuilder()
                            .appendPattern("yyyy-MM-dd[ HH:mm:ss]")
                            .optionalStart()
                            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                            .optionalEnd()
                            .toFormatter();
                    localDateTime = LocalDateTime.parse(value, formatter);
                }

                // Convert to epoch microseconds using the specified timezone
                ZonedDateTime zonedDateTime = localDateTime.atZone(effectiveZoneId);
                return zonedDateTime.toInstant().toEpochMilli() * 1000;
            }
        } catch (Exception e) {
            return null;
        }
    }

    /**
     * Overloaded method with default UTC timezone.
     */
    public static Long timestampToMicrosSinceEpochUTC(String value) {
        return timestampToMicrosSinceEpochUTC(value, ZoneOffset.UTC);
    }

    @Evolving
    public static final class Equal extends Predicate {
        public Equal(Expression left, Expression right) {
            super("=", Arrays.asList(left, right));
        }
    }

    @Evolving
    public static final class NotEqual extends Predicate {
        public NotEqual(Expression left, Expression right) {
            super("NOT", Arrays.asList(new Equal(left, right)));
        }
    }

    @Evolving
    public static final class GreaterThan extends Predicate {
        public GreaterThan(Expression left, Expression right) {
            super(">", Arrays.asList(left, right));
        }
    }

    @Evolving
    public static final class GreaterThanOrEqualTo extends Predicate {
        public GreaterThanOrEqualTo(Expression left, Expression right) {
            super(">=", Arrays.asList(left, right));
        }
    }

    @Evolving
    public static final class LessThan extends Predicate {
        public LessThan(Expression left, Expression right) {
            super("<", Arrays.asList(left, right));
        }
    }

    @Evolving
    public static final class CompareLessThanOrEqualTo extends Predicate {
        public CompareLessThanOrEqualTo(Expression left, Expression right) {
            super("<=", Arrays.asList(left, right));
        }
    }

    public static Expression toDeltaPredicate(JsonNode jsonPredicate) throws IOException {
        if(io.github.tanejagagan.sql.commons.Transformations.IS_CONSTANT.apply(jsonPredicate)) {
            return toLiteral(jsonPredicate);
        } else if (io.github.tanejagagan.sql.commons.Transformations.IS_REFERENCE.apply(jsonPredicate)) {
            return toReference(jsonPredicate);
        } else if(io.github.tanejagagan.sql.commons.Transformations.IS_COMPARISON.apply(jsonPredicate)){
            return toComparison(jsonPredicate);
        } else if(io.github.tanejagagan.sql.commons.Transformations.IS_CONJUNCTION_AND.apply(jsonPredicate) || io.github.tanejagagan.sql.commons.Transformations.IS_CONJUNCTION_OR.apply(jsonPredicate)  ) {
            return toConjunction(jsonPredicate);
        } else if(io.github.tanejagagan.sql.commons.Transformations.IS_CAST.apply(jsonPredicate)) {
            return toCast(jsonPredicate);
        }
        throw new UnsupportedOperationException("No transformation supported" + jsonPredicate);
    }

    private static Expression toComparison(JsonNode comparison) throws IOException {
        JsonNode left = comparison.get("left");
        JsonNode right = comparison.get("right");
        Expression leftExpr = toDeltaPredicate(left);
        Expression rightExpr = toDeltaPredicate(right);

        String comparisonType = comparison.get("type").asText();
        return switch (comparisonType) {
            case "COMPARE_EQUAL" -> new Equal(leftExpr, rightExpr);
            case "COMPARE_NOT_EQUAL" -> new NotEqual(leftExpr, rightExpr);
            case "COMPARE_GREATER_THAN" -> new GreaterThan(leftExpr, rightExpr);
            case "COMPARE_GREATERTHANOREQUALTO" -> new GreaterThanOrEqualTo(leftExpr, rightExpr);
            case "COMPARE_LESS_THAN" -> new LessThan(leftExpr, rightExpr);
            case "COMPARE_LESSTHANOREQUALTO"-> new CompareLessThanOrEqualTo(leftExpr, rightExpr);
            default -> throw new UnsupportedOperationException("Unsupported comparison type: " + comparisonType);
        };
    }

    private static Expression toConjunction(JsonNode conjunction) throws IOException {
        JsonNode children = conjunction.get("children");
        String conjunctionType = conjunction.get("type").asText();

        if (children.size() != 2) {
            throw new UnsupportedOperationException("Only binary conjunctions are supported");
        }

        Predicate leftPredicate = (Predicate) toDeltaPredicate(children.get(0));
        Predicate rightPredicate = (Predicate) toDeltaPredicate(children.get(1));

        if ("CONJUNCTION_AND".equals(conjunctionType)) {
            return new And(leftPredicate, rightPredicate);
        } else if ("CONJUNCTION_OR".equals(conjunctionType)) {
            return new Or(leftPredicate, rightPredicate);
        } else {
            throw new UnsupportedOperationException("Unsupported conjunction type: " + conjunctionType);
        }
    }

    // TODO
    private static Expression toLiteral(JsonNode literal) throws IOException {
        JsonNode value = literal.get("value");
        String type = value.get("type").get("id").asText();

        return switch (type) {
            case "INTEGER" -> Literal.ofInt(value.get("value").asInt());
            case "VARCHAR" -> Literal.ofString(value.get("value").asText());
            case "BOOLEAN" -> Literal.ofBoolean(value.get("value").asBoolean());
            case "DATE" -> Literal.ofDate(Integer.parseInt(value.get("value").toString()));
            case "TIMESTAMP" ->
                    Literal.ofTimestamp(Long.parseLong(value.get("value").toString()));
            case "FLOAT" -> Literal.ofFloat((float) value.get("value").asDouble());
            case "DOUBLE" -> Literal.ofDouble(value.get("value").asDouble());
            case "BLOB" -> Literal.ofBinary(value.get("value").textValue().getBytes(StandardCharsets.UTF_8));
            default -> throw new UnsupportedOperationException("Unsupported literal type: " + type);
        };
    }

    private static Expression toReference(JsonNode reference) {
        String columnName = reference.get("column_names").get(0).asText();
        return new Column(columnName);
    }

//    private static Expression castExpression(Expression expr, String sourceType, String targetType) {
//        if (castingFunctions.containsKey(targetType)) {
//            return castingFunctions.get(targetType).apply(expr);
//        } else {
//            throw new UnsupportedOperationException("Unsupported literal type: " + targetType);
//        }
//    }

    private static Expression toCast(JsonNode cast) throws IOException {
        System.out.println(cast.toPrettyString());
        JsonNode child = cast.get("child");
        Expression childExpr = toDeltaPredicate(child);
        String sourceType = String.valueOf(((Literal) childExpr).getDataType()).toUpperCase();;
        String castType = cast.get("cast_type").get("id").asText().toUpperCase();;

        if (!canCast(sourceType, castType)) {
            throw new UnsupportedOperationException("Casting from " + sourceType + " to " + castType + " is not supported");
        }
        return castExpression(childExpr, sourceType, castType);
    }
}
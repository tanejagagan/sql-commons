package io.github.tanejagagan.sql.commons.delta;

import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.annotation.Evolving;
import io.delta.kernel.expressions.*;
import io.delta.kernel.expressions.Column;
import io.delta.kernel.expressions.Literal;
import io.github.tanejagagan.sql.commons.Transformations;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Arrays;

import java.time.format.DateTimeFormatter;

public class DeltaTransformations {
    private static final DateTimeFormatter ISO_DATE = DateTimeFormatter.ISO_DATE;
    private static final DateTimeFormatter US_DATE = DateTimeFormatter.ofPattern("MM/dd/yyyy");
    private static final DateTimeFormatter DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final DateTimeFormatter US_DATETIME_FORMAT = DateTimeFormatter.ofPattern("MM/dd/yyyy HH:mm:ss");


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
    public static final class GREATERTHANOREQUALTO extends Predicate {
        public GREATERTHANOREQUALTO(Expression left, Expression right) {
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
    public static final class COMPARE_LESSTHANOREQUALTO extends Predicate {
        public COMPARE_LESSTHANOREQUALTO(Expression left, Expression right) {
            super("<=", Arrays.asList(left, right));
        }
    }

    public static Expression toDeltaPredicate(JsonNode jsonPredicate) {
        if(Transformations.IS_CONSTANT.apply(jsonPredicate)) {
            return toLiteral(jsonPredicate);
        } else if (Transformations.IS_REFERENCE.apply(jsonPredicate)) {
            return toReference(jsonPredicate);
        } else if(Transformations.IS_COMPARISON.apply(jsonPredicate)){
            return toComparison(jsonPredicate);
        } else if(Transformations.IS_CONJUNCTION_AND.apply(jsonPredicate) || Transformations.IS_CONJUNCTION_OR.apply(jsonPredicate)  ) {
            return toConjunction(jsonPredicate);
        } else if(Transformations.IS_CAST.apply(jsonPredicate)) {
            return toCast(jsonPredicate);
        }
        throw new UnsupportedOperationException("No transformation supported" + jsonPredicate);
    }

    private static Expression toComparison(JsonNode comparison) {
        JsonNode left = comparison.get("left");
        JsonNode right = comparison.get("right");
        Expression leftExpr = toDeltaPredicate(left);
        Expression rightExpr = toDeltaPredicate(right);

        String comparisonType = comparison.get("type").asText();
        return switch (comparisonType) {
            case "COMPARE_EQUAL" -> new Equal(leftExpr, rightExpr);
            case "COMPARE_NOT_EQUAL" -> new NotEqual(leftExpr, rightExpr);
            case "COMPARE_GREATER_THAN" -> new GreaterThan(leftExpr, rightExpr);
            case "COMPARE_GREATERTHANOREQUALTO" -> new GREATERTHANOREQUALTO(leftExpr, rightExpr);
            case "COMPARE_LESS_THAN" -> new LessThan(leftExpr, rightExpr);
            case "COMPARE_LESSTHANOREQUALTO"-> new COMPARE_LESSTHANOREQUALTO(leftExpr, rightExpr);
            default -> throw new UnsupportedOperationException("Unsupported comparison type: " + comparisonType);
        };
    }

    private static Expression toConjunction(JsonNode conjunction) {
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
    private static Expression toLiteral(JsonNode literal) {
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
            default -> throw new UnsupportedOperationException("Unsupported literal type: " + type);
        };
    }

    private static Expression toReference(JsonNode reference) {
        String columnName = reference.get("column_names").get(0).asText();
        return new Column(columnName);
    }

    private static Expression toCast(JsonNode cast) {
        System.out.println(cast.toPrettyString());
        JsonNode child = cast.get("child");
        Expression childExpr = toDeltaPredicate(child);
        String castType = cast.get("cast_type").get("id").asText();

        return switch (castType) {
            case "INTEGER" -> Literal.ofInt(Integer.parseInt(childExpr.toString()));
            case "VARCHAR" -> Literal.ofString(childExpr.toString());
            case "BOOLEAN" -> Literal.ofBoolean(Boolean.parseBoolean(childExpr.toString()));
            case "DATE" -> handleDateLiteral(childExpr.toString()); // Assuming date is in "yyyy-MM-dd" format
            case "TIMESTAMP" ->
                    Literal.ofTimestamp(Long.parseLong(childExpr.toString())); // Assuming timestamp is in milliseconds
            case "FLOAT" -> Literal.ofFloat(Float.parseFloat(childExpr.toString()));
            case "DOUBLE" -> Literal.ofDouble(Double.parseDouble(childExpr.toString()));
            default -> throw new UnsupportedOperationException("Unsupported literal type: " + castType);
        };
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
}
package io.github.tanejagagan.sql.commons.delta;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.delta.kernel.expressions.Expression;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.SQLException;

public class TransformationsTest {
    @Test
    public void testTransformation() throws SQLException, IOException {
//        String sql = "select a, b from t where x = 10 or y = cast('2025-01-01' as date)";
        String sql = "select a, b from t where CAST('Spark' AS BINARY)";
        JsonNode tree = io.github.tanejagagan.sql.commons.Transformations.parseToTree(sql);
        ArrayNode statements = (ArrayNode)tree.get("statements");
        JsonNode firstStatement =  statements.get(0);
        JsonNode whereClause = firstStatement.get("node").get("where_clause");
        System.out.println(whereClause.toPrettyString());
        Expression predicate = Transformations.toDeltaPredicate(whereClause);
        System.out.println(predicate);
    }
}

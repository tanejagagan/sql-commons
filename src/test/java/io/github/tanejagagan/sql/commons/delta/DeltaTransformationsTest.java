package io.github.tanejagagan.sql.commons.delta;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import io.delta.kernel.expressions.Expression;
import io.github.tanejagagan.sql.commons.Transformations;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;

public class DeltaTransformationsTest {
    @Test
    public void testTransformation() throws SQLException, JsonProcessingException {
        String sql = "select a, b from t where x = 10 and y = cast('2025-01-01' as date)";
        JsonNode tree = Transformations.parseToTree(sql);
        ArrayNode statements = (ArrayNode)tree.get("statements");
        JsonNode firstStatement =  statements.get(0);
        JsonNode whereClause = firstStatement.get("node").get("where_clause");
        System.out.println(whereClause.toPrettyString());
        Expression predicate = DeltaTransformations.toDeltaPredicate(whereClause);
        System.out.println(predicate);
    }
}

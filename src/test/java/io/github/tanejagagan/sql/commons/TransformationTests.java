package io.github.tanejagagan.sql.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TransformationTests {

    @Test
    public void testSplitStatements() throws SQLException, JsonProcessingException {
        String sql = " select * from generate_series(10); \n" +
                "select * from generate_series(11);";
        JsonNode node = Transformations.parseToTree(sql);
        List<JsonNode> statements = Transformations.splitStatements(node);
        for(JsonNode n : statements) {
            String s = Transformations.parseToSql(n);
            ConnectionPool.execute(s);
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"select * from t where x in (select y from t2) "})
    public void testSubQueries(String sql) throws SQLException, JsonProcessingException {
        var jsonNode = Transformations.parseToTree(sql);
        var statements = (ArrayNode) jsonNode.get("statements");
        var statement = statements.get(0);
        var statementNode = statement.get("node");
        var where = statementNode.get("where_clause");
        var qs = Transformations.collectSubQueries(where );
        Assertions.assertEquals(1, qs.size());
    }
}

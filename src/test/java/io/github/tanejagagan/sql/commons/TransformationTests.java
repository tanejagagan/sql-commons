package io.github.tanejagagan.sql.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.List;

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
}

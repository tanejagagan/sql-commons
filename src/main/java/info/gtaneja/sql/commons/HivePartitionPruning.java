package info.gtaneja.sql.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.sql.SQLException;
import java.util.List;
import java.util.Set;

public class HivePartitionPruning extends PartitionPruning {

    public static void main(String[] args) throws SQLException, JsonProcessingException {
        String sql0 = "select * from t";
        String sql1 = "select * from t where c1 = 10 and p1 = 50";
        String sql2 = "select * from t where true and p1 = 50";
        String sql4 = "select * from t where c1 = 10 or p1 = 50";
        String sql5 = "select * from t where c1 = 10";

        for (String sql : List.of(sql0, sql1, sql2, sql4, sql5)) {
            JsonNode tree = Transformations.parseToTree(sql);
            JsonNode newTree = Transformations.transform(tree, Transformations.IS_SELECT,
                    Transformations.removeNonPartitionColumnsPredicatesInQuery("stat_t", Set.of("p1")));
            System.out.printf("SQL : %s \n", sql);
            System.out.printf("Transformed SQL : %s", Transformations.parseToSql(newTree));
        }
    }
}



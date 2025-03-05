package info.gtaneja.sql.commons;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * Lower bound
 * String input_1_0 = "select * from t where c1 > 10";
 * String input_1_1 = "select * from t where  10 < c1";
 * String input_1_2 = "select * from t where c1 >= 10";
 * String input_1_3 = "select * from t where  10 <= c1";
 * String input_1_4 = "select * from t where c1 = 10;
 * String input_1_4 = "select * from t where 10 = c1;
 * String expected_1 = view +
 * "select filename from stat_t where if( min_c1=null, true, cast(min_c1 as int) >= cast(10 as int))";
 * Upper bound
 * String input_2_0 = "select * from t where c1 < 10";
 * String input_2_1 = "select * from t where  10 > c1";
 * String input_2_2 = "select * from t where c1 <= 10";
 * String input_2_3 = "select * from t where  10 >= c1";
 * String input_1_4 = "select * from t where c1 = 10;
 * String input_1_4 = "select * from t where 10 = c1;
 * String expected_2 = view +
 * "select filename from stat_t where if( min_c1=null, true, cast(max_c1 as int) <= cast(10 as int))";
 **/

public class DeltaLakePartitionPruning extends PartitionPruning {
    public static void main(String[] args) throws SQLException, JsonProcessingException {
        String sql_1 = "select * from t where c1 = 10";
        String sql0 = "select * from t";
        String sql1 = "select * from t where c1 = 10 and p1 = 50";
        String sql2 = "select * from t where true and p1 = 50";
        String sql4 = "select * from t where c1 = 10 or p1 = 50";
        String sql5 = "select * from t where true and c1 = 10";

        Map<String, String> minMap = Map.of("c1", "c1_min", "p1", "p1_min");
        Map<String, String> maxMap = Map.of("c1", "c1_min", "p1", "p1_min");
        Map<String, String> dataTypes = Map.of("c1", "INTEGER", "p1", "INTEGER");

        for (String sql : List.of(sql0, sql1, sql2, sql4, sql5)) {
            JsonNode tree = Transformations.parseToTree(sql);
            JsonNode newTree = Transformations.transform(tree, Transformations.IS_SELECT,
                    Transformations.replaceEqualMinMaxInQuery("t_stat", minMap, maxMap, dataTypes));
            System.out.printf("SQL : %s \n", sql);
            System.out.printf("Transformed SQL : %s", Transformations.parseToSql(newTree));
        }
    }
}

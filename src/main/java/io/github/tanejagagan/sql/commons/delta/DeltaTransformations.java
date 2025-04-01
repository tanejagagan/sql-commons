package io.github.tanejagagan.sql.commons.delta;

import com.fasterxml.jackson.databind.JsonNode;
import io.delta.kernel.expressions.*;
import io.github.tanejagagan.sql.commons.Transformations;

public class DeltaTransformations {
    public static Expression toDeltaPredicate(JsonNode jsonPredicate) {
        if(Transformations.IS_CONSTANT.apply(jsonPredicate)) {
            return toLiteral(jsonPredicate);
        } else if (Transformations.IS_REFERENCE.apply(jsonPredicate)) {
            return toReference(jsonPredicate);
        } else if(Transformations.IS_COMPARISON.apply(jsonPredicate)){
            return null;
        } else if(Transformations.IS_CONJUNCTION_AND.apply(jsonPredicate)) {
            return null;
        } else if(Transformations.IS_CAST.apply(jsonPredicate)) {
            return toCast(jsonPredicate);
        }
        throw new UnsupportedOperationException("No transformation supported" + jsonPredicate);
    }

    // TODO
    private static Expression toLiteral(JsonNode literal) {
        return null;
    }

    // TODO
    private static Expression toReference(JsonNode reference) {
        return null;
    }

    // TODO
    private static Expression toCast(JsonNode cast){
        return null;
    }
}

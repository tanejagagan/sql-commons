package io.github.tanejagagan.sql.commons;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface Extractor<T> {

    T extract(ResultSet rs) throws SQLException;
    
    default T apply(ResultSet rs) {
        try {
            return extract(rs);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

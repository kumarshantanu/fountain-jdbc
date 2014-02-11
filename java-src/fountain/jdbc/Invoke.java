package fountain.jdbc;

import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

/**
 * This invoker class is used only to avoid reflection warning in Clojure code.
 */
public class Invoke {

    public static int[] jtBatchUpdate(JdbcTemplate jt, String sql, List<Object[]> batchArgs) {
        return jt.batchUpdate(sql, batchArgs);
    }

    public static int[] njtBatchUpdate(NamedParameterJdbcTemplate njt, String sql,
            Map<String, ?>[] batchValues) {
        return njt.batchUpdate(sql, batchValues);
    }

}

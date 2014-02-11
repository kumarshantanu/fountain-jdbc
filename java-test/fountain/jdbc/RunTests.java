package fountain.jdbc;

import java.util.Arrays;
import java.util.List;

import mikera.cljunit.ClojureTest;

public class RunTests extends ClojureTest {

    @Override
    public List<String> namespaces() {
        return Arrays.asList(new String[] {
                "fountain.jdbc.ops-test"
        });
    }

}

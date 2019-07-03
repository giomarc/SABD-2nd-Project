package erreesse.operators.keyby;

import org.apache.flink.api.java.functions.KeySelector;

@Deprecated
public class KeyByValue implements KeySelector<Integer, Integer> {
    @Override
    public Integer getKey(Integer value) throws Exception {
        return value;
    }
}

package erreesse.operators.keyby;

import erreesse.metrics.LatencyTuple3;
import org.apache.flink.api.java.functions.KeySelector;


public class KeyByWindowStart implements KeySelector<LatencyTuple3<Long,String,Long>,Long> {
    @Override
    // timestamp, articleId, count
    public Long getKey(LatencyTuple3<Long,String,Long> t3) throws Exception {
        return t3._1();
    }
}

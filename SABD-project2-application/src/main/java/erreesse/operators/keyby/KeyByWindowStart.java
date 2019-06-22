package erreesse.operators.keyby;

import org.apache.flink.api.java.functions.KeySelector;
import scala.Tuple3;


public class KeyByWindowStart implements KeySelector<Tuple3<Long,String,Long>,Long> {
    @Override
    // timestamp, articleId, count
    public Long getKey(Tuple3<Long, String, Long> t3) throws Exception {
        return t3._1();
    }
}

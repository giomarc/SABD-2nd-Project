package erreesse.operators.keyby;

import org.apache.flink.api.java.functions.KeySelector;
import scala.Tuple3;
import scala.Tuple4;


public class KeyByWindowStart implements KeySelector<Tuple4<Long,String,Long,Long>,Long> {
    @Override
    // timestamp, articleId, count
    public Long getKey(Tuple4<Long,String,Long,Long> t4) throws Exception {
        return t4._1();
    }
}

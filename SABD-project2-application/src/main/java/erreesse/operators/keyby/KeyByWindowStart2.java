package erreesse.operators.keyby;

import org.apache.flink.api.java.functions.KeySelector;
import scala.Tuple2;
import scala.Tuple3;

/*
* IN -> Tuple2<TimestampWindowStart,Counter>
* */
public class KeyByWindowStart2 implements KeySelector<Tuple3<Long,Long,Integer>,Long>  {
    @Override
    public Long getKey(Tuple3<Long, Long,Integer> t3) throws Exception {
        return t3._1();
    }
}

package erreesse.operators.processwindowfunctions;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

/*
* IN
* OUT
* KEY
* TW
*
* */
public class ArticleCounterProcessWF extends ProcessWindowFunction<Tuple2<Long,Long>, Tuple4<Long,String,Long,Long>,String, TimeWindow> {
    @Override
    public void process(String key,
                        Context context,
                        Iterable<Tuple2<Long,Long>> elements,
                        Collector<Tuple4<Long,String,Long,Long>> out) throws Exception {

        long timestamp = context.window().getStart();
        long count = elements.iterator().next()._1;

        long lastTupleOfWindowTimestamp = elements.iterator().next()._2;

        out.collect(new Tuple4<>(timestamp,key,count,lastTupleOfWindowTimestamp));

    }
}

package erreesse.operators.processwindowfunctions;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple3;

/*
* IN
* OUT
* KEY
* TW
*
* */
public class ArticleCounterProcessWF extends ProcessWindowFunction<Long, Tuple3<Long,String,Long>,String, TimeWindow> {
    @Override
    public void process(String key,
                        Context context,
                        Iterable<Long> elements,
                        Collector<Tuple3<Long, String, Long>> out) throws Exception {

        long timestamp = context.window().getStart();
        long count = elements.iterator().next();

        out.collect(new Tuple3<>(timestamp,key,count));

    }
}

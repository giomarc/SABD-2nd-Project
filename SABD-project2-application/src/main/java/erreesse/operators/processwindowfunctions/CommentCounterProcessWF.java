package erreesse.operators.processwindowfunctions;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import scala.Tuple3;

/*
 * IN
 * OUT -> Tuple2<Timestamp,Count>
 * KEY
 * TW
 *
 * */
public class CommentCounterProcessWF extends ProcessWindowFunction<Long, Tuple3<Long,Long,Integer>,Integer, TimeWindow> {
    @Override
    public void process(
            Integer key,
            Context context,
            Iterable<Long> elements,
            Collector<Tuple3<Long, Long, Integer>> out) throws Exception {



        long timestamp = context.window().getStart();
        long count = elements.iterator().next();

        out.collect(new Tuple3<>(timestamp,count,key));


    }

}

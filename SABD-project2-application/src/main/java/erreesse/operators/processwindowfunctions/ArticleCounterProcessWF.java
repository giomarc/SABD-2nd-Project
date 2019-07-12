package erreesse.operators.processwindowfunctions;

import erreesse.metrics.LatencyTuple1;
import erreesse.metrics.LatencyTuple3;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/*
* IN
* OUT
* KEY
* TW
*
* */
public class ArticleCounterProcessWF extends ProcessWindowFunction<LatencyTuple1<Long>, LatencyTuple3<Long,String,Long>,String, TimeWindow> {
    @Override
    public void process(String key,
                        Context context,
                        Iterable<LatencyTuple1<Long>> elements,
                        Collector<LatencyTuple3<Long,String,Long>> out) throws Exception {

        long timestamp = context.window().getStart();
        long count = elements.iterator().next()._1;

        long lastTupleOfWindowTimestamp = elements.iterator().next().getStartTime();
        LatencyTuple3 result = new LatencyTuple3<>(timestamp,key,count);
        result.setStartTime(lastTupleOfWindowTimestamp);


        out.collect(result);

    }
}

package erreesse.operators.processwindowfunctions;

import erreesse.operators.aggregator.FasciaArrayAccumulator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple3;

// Tuple3<Timestamp,Counter,Fascia>
public class FasciaProcessWindowFunction extends ProcessAllWindowFunction<FasciaArrayAccumulator, String, TimeWindow> {

    @Override
    public void process(Context context,
                        Iterable<FasciaArrayAccumulator> iterable,
                        Collector<String> collector) throws Exception {

        long timestamp = context.window().getStart();
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp);

        FasciaArrayAccumulator acc = iterable.iterator().next();
        for (int i =0; i< 12; i++) {
            sb.append(",");
            sb.append(acc.getCountByIndex(i));
        }

        collector.collect(sb.toString());

    }
}

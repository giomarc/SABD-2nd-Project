package erreesse.operators.processwindowfunctions;

import erreesse.configuration.AppConfiguration;
import erreesse.metrics.LatencyTuple1;
import erreesse.operators.aggregator.FasciaArrayAccumulator;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

// IN -> LatencyTuple1<FasciaArrayAccumulator>
// OUT -> String
public class FasciaProcessWindowFunction extends ProcessAllWindowFunction<LatencyTuple1<FasciaArrayAccumulator>, String, TimeWindow> {

    private transient long windowLatency = 0;

    @Override
    public void process(Context context,
                        Iterable<LatencyTuple1<FasciaArrayAccumulator>> iterable,
                        Collector<String> collector) throws Exception {

        long timestamp = context.window().getStart();
        StringBuilder sb = new StringBuilder();
        sb.append(timestamp);


        LatencyTuple1<FasciaArrayAccumulator> latAcc = iterable.iterator().next();
        FasciaArrayAccumulator acc = latAcc._1;

        computeQueryLatency(latAcc.getStartTime());

        for (int i =0; i< 12; i++) {
            sb.append(",");
            sb.append(acc.getCountByIndex(i));
        }
        if (AppConfiguration.PRINT_LATENCY_METRIC) {
            sb.append("|lat:"+windowLatency+"ms");
        }



        collector.collect(sb.toString());

    }

    private void computeQueryLatency(long lastPTinAllWindow) {
        windowLatency = 0;
        if (lastPTinAllWindow > 0) {
            long elapsedTime = System.nanoTime() - lastPTinAllWindow;
            windowLatency = elapsedTime/1000000;
        }

    }
}

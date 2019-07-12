package erreesse.operators.apply;

import erreesse.configuration.AppConfiguration;
import erreesse.metrics.LatencyTuple3;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Comparator;
import java.util.PriorityQueue;

/*
* IN --> Tuple3<Long,String,Long> --> <Timestamp,ArticleID,Count>
* OUT
* KEY
* TW
* */
public class RankingWF implements WindowFunction<LatencyTuple3<Long, String, Long>, String, Long, TimeWindow> {
    private transient long windowLatency = 0;

    @Override
    public void apply(Long key,
                      TimeWindow timeWindow,
                      Iterable<LatencyTuple3<Long,String,Long>> iterable,
                      Collector<String> out) throws Exception {

        long lastPTinAllWindow = 0;


        final int QUEUE_SIZE = 3;
        Comparator<Tuple2<String,Long>> comparator = (t1, t2) -> (t2._2.compareTo(t1._2));
        PriorityQueue<Tuple2<String,Long>> ordset = new PriorityQueue<>(QUEUE_SIZE,comparator);

        for (LatencyTuple3<Long,String,Long> t3 : iterable) {

            // find max timestamp
            long lastPTinWindow = t3.getStartTime();
            lastPTinAllWindow = Math.max(lastPTinAllWindow,lastPTinWindow);

            ordset.add(new Tuple2<>(t3._2(),t3._3()));
        }

        StringBuilder sb = new StringBuilder();
        sb.append(key);

        computeQueryLatency(lastPTinAllWindow);

        long size = Math.min(QUEUE_SIZE, ordset.size());

        for (int i =0; i< size; i++) {
            Tuple2<String, Long> ranked = ordset.poll();
            sb.append(","+ranked._1+","+ranked._2);
        }
        if (AppConfiguration.PRINT_LATENCY_METRIC) {
            sb.append("|lat:"+windowLatency+"ms");
        }

        out.collect(sb.toString());

    }

    private void computeQueryLatency(long lastPTinAllWindow) {
        windowLatency = 0;
        if (lastPTinAllWindow > 0) {
            long elapsedTime = System.nanoTime() - lastPTinAllWindow;
            windowLatency = elapsedTime/1000000;
        }

    }
}

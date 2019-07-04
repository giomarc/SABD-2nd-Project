package erreesse.operators.windowfunctions;

import erreesse.metrics.LatencyTuple2;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Comparator;
import java.util.PriorityQueue;

public class ComputeMostPopularUserWF implements AllWindowFunction<LatencyTuple2<Long, Double>, String, TimeWindow> {
    protected transient long maxTimeStamp = 0L;
    protected transient long queryLatency = 0L;


    private void updateMaxTimeStamp(LatencyTuple2<Long, Double> t2) {
        maxTimeStamp = Math.max(maxTimeStamp, t2.getStartTime());
    }

    private void computeQueryLatency(long lastPTinAllWindow) {
        queryLatency = 0L;
        if (lastPTinAllWindow > 0) {
            long elapsedTime = System.nanoTime() - lastPTinAllWindow;
            queryLatency = elapsedTime/1000000;
        }
    }

    @Override
    public void apply(TimeWindow timeWindow,
                      Iterable<LatencyTuple2<Long, Double>> iterable,
                      Collector<String> out) throws Exception {

        long timeStamp = timeWindow.getStart();

        final int QUEUE_SIZE = 10;
        Comparator<Tuple2<Long, Double>> comparator = (t1, t2) -> (t2._2.compareTo(t1._2));
        PriorityQueue<Tuple2<Long, Double>> ordset = new PriorityQueue<>(QUEUE_SIZE,comparator);


        for (LatencyTuple2<Long, Double> t2 : iterable) {
            ordset.add(t2);
            updateMaxTimeStamp(t2);
        }
        computeQueryLatency(maxTimeStamp);

        StringBuilder sb = new StringBuilder();
        sb.append(timeStamp);

        long size = Math.min(10, ordset.size());

        for (int i = 0; i < size; i++) {
            Tuple2<Long, Double> ranked = ordset.poll();
            String ap = String.format(",%d,%.2f",ranked._1,ranked._2);
            sb.append(ap);
        }

        sb.append("|lat:"+ queryLatency);

        out.collect(sb.toString());


    }
}

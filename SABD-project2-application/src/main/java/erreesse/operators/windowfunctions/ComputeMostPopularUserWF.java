package erreesse.operators.windowfunctions;

import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Comparator;
import java.util.TreeSet;

public class ComputeMostPopularUserWF implements AllWindowFunction<Tuple2<Long, Double>, String, TimeWindow> {
    @Override
    public void apply(TimeWindow timeWindow,
                      Iterable<Tuple2<Long, Double>> iterable,
                      Collector<String> out) throws Exception {

        long timeStamp = timeWindow.getStart();

        Comparator<Tuple2<Long, Double>> comparator = Comparator.comparing(t -> t._2);
        TreeSet<Tuple2<Long, Double>> ordset = new TreeSet<>(comparator);

        for (Tuple2<Long, Double> t2 : iterable) {
            ordset.add(t2);
        }

        StringBuilder sb = new StringBuilder();
        sb.append(timeStamp);

        long size = Math.min(10, ordset.size());

        for (int i = 0; i < size; i++) {
            Tuple2<Long, Double> ranked = ordset.pollLast();
            String ap = String.format(",%d,%.2f",ranked._1,ranked._2);
            sb.append(ap);
        }

        out.collect(sb.toString());


    }
}

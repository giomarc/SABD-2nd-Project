package erreesse.operators.processwindowfunctions;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Comparator;
import java.util.TreeSet;

/*
* IN
* OUT
* KEY
* TW
*
* */
public class RankingPWF extends ProcessWindowFunction<Tuple3<Long,String,Long>,String,Long, TimeWindow> {
    @Override
    public void process(Long key,
                        Context ctx,
                        Iterable<Tuple3<Long, String, Long>> iterable,
                        Collector<String> out) throws Exception {


        Comparator<Tuple2<String,Long>> comparator = (t1, t2) -> (t1._2.compareTo(t2._2));
        TreeSet<Tuple2<String,Long>> ordset = new TreeSet<>(comparator);

        for (Tuple3<Long, String, Long> t3 : iterable) {
            ordset.add(new Tuple2<>(t3._2(),t3._3()));
        }

        StringBuilder sb = new StringBuilder();
        sb.append(key);

        long size = Math.min(3, ordset.size());

        for (int i =0; i< size; i++) {
            Tuple2<String, Long> ranked = ordset.pollLast();
            sb.append(","+ranked._1+","+ranked._2);
        }

        out.collect(sb.toString());

    }
}

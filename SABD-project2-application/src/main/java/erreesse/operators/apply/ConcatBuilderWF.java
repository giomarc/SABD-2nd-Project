package erreesse.operators.apply;

import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Comparator;
import java.util.TreeSet;


/*
 * IN --> Tuple3<Long,Long,Integer>  <TimestampWindowStart,Count,Hour2Slot>
 * OUT
 * KEY
 * TW
 * */

public class ConcatBuilderWF implements WindowFunction<Tuple3<Long,Long,Integer>,String,Long, TimeWindow> {
    @Override
    public void apply(Long key,
                      TimeWindow timeWindow,
                      Iterable<Tuple3<Long, Long,Integer>> elements,
                      Collector<String> out) throws Exception {

        StringBuilder sb = new StringBuilder();
        sb.append(key);

        Comparator<Tuple2<Integer,Long>> comparator = (t1, t2) -> (t1._1.compareTo(t2._1));
        TreeSet<Tuple2<Integer,Long>> ordset = new TreeSet<>(comparator);

        for (Tuple3<Long, Long, Integer> t3 : elements) {
            ordset.add(new Tuple2<>(t3._3(),t3._2()));
        }

        for (Tuple2<Integer, Long> t2 : ordset) {
            sb.append(",fascia:"+t2._1+"|count:"+t2._2);
            //sb.append(","+t2._2);
        }

        out.collect(sb.toString());
    }
}

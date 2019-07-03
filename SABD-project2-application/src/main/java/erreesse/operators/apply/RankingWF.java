package erreesse.operators.apply;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.TreeSet;

/*
* IN --> Tuple4<Long,String,Long,Long> --> <Timestamp,ArticleID,Count,lastPTinWindow>
* OUT
* KEY
* TW
* */
public class RankingWF extends RichWindowFunction<Tuple4<Long, String, Long, Long>, String, Long, TimeWindow> {

    private transient Meter meter;

    @Override
    public void open(Configuration config) {
        com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();

        this.meter = getRuntimeContext()
                .getMetricGroup()
                .meter("EsseErreMeterRankingWF", new DropwizardMeterWrapper(dropwizardMeter));
    }

    @Override
    public void apply(Long key,
                      TimeWindow timeWindow,
                      Iterable<Tuple4<Long, String, Long,Long>> iterable,
                      Collector<String> out) throws Exception {

        long lastPTinAllWindow = 0;


        final int QUEUE_SIZE = 3;
        Comparator<Tuple2<String,Long>> comparator = (t1, t2) -> (t2._2.compareTo(t1._2));
        PriorityQueue<Tuple2<String,Long>> ordset = new PriorityQueue<>(QUEUE_SIZE,comparator);

        long eventCounter = 0;
        for (Tuple4<Long, String, Long,Long> t4 : iterable) {
            eventCounter++;
            long lastPTinWindow = t4._4();
            lastPTinAllWindow = Math.max(lastPTinAllWindow,lastPTinWindow);

            ordset.add(new Tuple2<>(t4._2(),t4._3()));
        }

        meter.markEvent(eventCounter);

        StringBuilder sb = new StringBuilder();
        sb.append(key);

        long size = Math.min(QUEUE_SIZE, ordset.size());

        for (int i =0; i< size; i++) {
            Tuple2<String, Long> ranked = ordset.poll();
            sb.append(","+ranked._1+","+ranked._2);
        }

        long windowLatency = System.nanoTime() - lastPTinAllWindow;
        windowLatency = windowLatency/1000000;
        sb.append("|lat:"+windowLatency+" ms");

        out.collect(sb.toString());

    }
}

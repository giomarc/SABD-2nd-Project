package erreesse.query;

import erreesse.counter.ArticleCounter;
import erreesse.datasource.CommentInfoSource;
import erreesse.pojo.CommentInfoPOJO;
import erreesse.time.ApproveDateTimeAssigner;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Set;
import java.util.TreeSet;

public class Query1 {

    public static void main(String[] args) {

        // set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        /*DataStream<Tuple3<Long, String, Long>> finalResult = env
                .addSource(new CommentInfoSource())
                .map(line -> CommentInfoPOJO.parseFromStringLine(line))
                .assignTimestampsAndWatermarks(new ApproveDateTimeAssigner())
                .map(cip -> new Tuple2<>(cip, 1L))
                .keyBy((KeySelector<Tuple2<CommentInfoPOJO, Long>, String>) commentInfoPOJOLongTuple2 -> commentInfoPOJOLongTuple2.f0.getArticleID())
                .timeWindow(Time.hours(1))
                .reduce(
                        (ReduceFunction<Tuple2<CommentInfoPOJO, Long>>)
                                (t0, t1) -> new Tuple2<>(t0.f0, t0.f1 + t1.f1),
                        (WindowFunction<Tuple2<CommentInfoPOJO, Long>, Tuple3<Long, String, Long>, String, TimeWindow>)
                                (key, window, inputs, out) -> {

                                long windowStart = window.getStart();

                                for (Tuple2<CommentInfoPOJO, Long> input : inputs) {
                                    out.collect(new Tuple3<>(windowStart, input.f0.getArticleID(), input.f1));
                                }
                            }
                );*/

        env
                .addSource(new CommentInfoSource())
                .map(line -> CommentInfoPOJO.parseFromStringLine(line))
                .assignTimestampsAndWatermarks(new ApproveDateTimeAssigner())
                .keyBy("articleID")
                .timeWindow(Time.hours(1))
                .aggregate(new AggregateFunction<CommentInfoPOJO, Tuple2<String,Long>, Tuple2<String,Long>>() {

                    @Override
                    public Tuple2<String, Long> createAccumulator() {
                        return new Tuple2<>("",1L);
                    }

                    @Override
                    public Tuple2<String, Long> add(CommentInfoPOJO commentInfoPOJO, Tuple2<String, Long> t2) {
                        return new Tuple2<>(commentInfoPOJO.getArticleID(),t2.f1+1);
                    }

                    @Override
                    public Tuple2<String, Long> getResult(Tuple2<String, Long> t1) {
                        return t1;
                    }

                    @Override
                    public Tuple2<String, Long> merge(Tuple2<String, Long> t1, Tuple2<String, Long> acc1) {
                        return new Tuple2<>(t1.f0,t1.f1+acc1.f1);
                    }
                }).print();
                /*.process(new ProcessWindowFunction<CommentInfoPOJO, Tuple2<String, Long>, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple,
                                        Context context,
                                        Iterable<CommentInfoPOJO> iterable,
                                        Collector<Tuple2<String, Long>> out) throws Exception {
                        long i = 1L;
                        for (CommentInfoPOJO cip : iterable) {
                            out.collect(new Tuple2<>(cip.getArticleID(), i));
                            i++;
                        }
                    }
                });*/

        //finalResult.print();

        try {
            env.execute("Test KafkaConnector");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

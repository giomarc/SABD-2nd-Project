package erreesse.query;

import erreesse.datasource.CommentInfoSource;
import erreesse.operators.aggregator.CommentCounterAggregator;
import erreesse.operators.apply.ConcatBuilderWF;
import erreesse.operators.keyby.KeyByValue;
import erreesse.operators.keyby.KeyByWindowStart2;
import erreesse.operators.map.TwoHourMapFunction;
import erreesse.operators.windowfunctions.CommentCounterProcessWF;
import erreesse.pojo.CommentInfoPOJO;
import erreesse.time.DateTimeAscendingAssigner;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Comparator;
import java.util.NoSuchElementException;
import java.util.TreeSet;

public class Query3 {
    public static final int IS_DIRECT = 1;

    public static void main(String[] args) {


        // set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        SingleOutputStreamOperator<CommentInfoPOJO> originalStream = env
                .addSource(new CommentInfoSource())
                .map(line -> CommentInfoPOJO.parseFromStringLine(line))
                .assignTimestampsAndWatermarks(new DateTimeAscendingAssigner());


        SingleOutputStreamOperator<CommentInfoPOJO> directComment = originalStream.filter(cip -> cip.getDepth() == IS_DIRECT);
        SingleOutputStreamOperator<CommentInfoPOJO> indirectComment = originalStream.filter(cip -> cip.getDepth() != IS_DIRECT);

        CoGroupedStreams<CommentInfoPOJO, CommentInfoPOJO>.Where<String>.EqualTo cogroupedStreams =
                directComment.coGroup(indirectComment)
                .where((KeySelector<CommentInfoPOJO, String>) commentInfoPOJO -> commentInfoPOJO.getUserDisplayName())
                .equalTo((KeySelector<CommentInfoPOJO, String>) commentInfoPOJO -> commentInfoPOJO.getParentUserDisplayName());

        SingleOutputStreamOperator<String> hourStream = cogroupedStreams
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new CoGroupFunction<CommentInfoPOJO, CommentInfoPOJO, Tuple2<String,Double>>() {
                    @Override
                    public void coGroup(Iterable<CommentInfoPOJO> iterableA,
                                        Iterable<CommentInfoPOJO> iterableB,
                                        Collector<Tuple2<String, Double>> out) throws Exception {

                        long totalLike = 0L;
                        for (CommentInfoPOJO singleCip : iterableA) {
                            totalLike += singleCip.getRecommendations();
                        }

                        long b = 0L;
                        for (CommentInfoPOJO singleCip : iterableB) {
                            b++;
                        }

                        double finalResult = 0.3 * totalLike + 0.7 * b;

                        CommentInfoPOJO nextA = null;
                        CommentInfoPOJO nextB = null;
                        String key = null;
                        try {
                            nextA = iterableA.iterator().next();
                            nextB = iterableB.iterator().next();
                        } catch (NoSuchElementException e) {

                        }
                        if (nextA != null) key = nextA.getUserDisplayName();
                        if (nextB != null) key = nextB.getParentUserDisplayName();

                        out.collect(new Tuple2<>(key, finalResult));

                    }
                })
                .timeWindowAll(Time.days(1))
                .apply(new AllWindowFunction<Tuple2<String, Double>, String, TimeWindow>() {
                    @Override
                    public void apply(TimeWindow timeWindow,
                                      Iterable<Tuple2<String, Double>> iterable,
                                      Collector<String> out) throws Exception {

                        long timeStamp = timeWindow.getStart();

                        Comparator<Tuple2<String, Double>> comparator = Comparator.comparing(t -> t._2);
                        TreeSet<Tuple2<String, Double>> ordset = new TreeSet<>(comparator);

                        for (Tuple2<String, Double> t2 : iterable) {
                            ordset.add(t2);
                        }

                        StringBuilder sb = new StringBuilder();
                        sb.append(timeStamp);

                        long size = Math.min(10, ordset.size());

                        for (int i = 0; i < size; i++) {
                            Tuple2<String, Double> ranked = ordset.pollLast();
                            sb.append("," + ranked._1 + "," + ranked._2);
                        }

                        out.collect(sb.toString());


                    }
                });

        hourStream.writeAsText("/sabd/result/query3/1day.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("Query2");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

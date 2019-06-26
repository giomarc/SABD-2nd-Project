package erreesse.query;

import erreesse.datasource.CommentInfoSource;
import erreesse.operators.aggregator.CommentCounterAggregator;
import erreesse.operators.apply.ConcatBuilderWF;
import erreesse.operators.keyby.KeyByValue;
import erreesse.operators.keyby.KeyByWindowStart2;
import erreesse.operators.map.TwoHourMapFunction;
import erreesse.operators.windowassigner.MonthWindowAssigner;
import erreesse.operators.processwindowfunctions.CommentCounterProcessWF;
import erreesse.pojo.CommentInfoPOJO;
import erreesse.time.DateTimeAscendingAssigner;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Query2 {
    public static final int IS_DIRECT = 1;

    public static void main(String[] args) {


        // set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // KeyedStream<V, K>
        KeyedStream<Integer, Integer> originalStream = env
                .addSource(new CommentInfoSource())
                .map(line -> CommentInfoPOJO.parseFromStringLine(line))
                // filtro solo i commenti diretti
                //.filter(cip -> cip.getDepth() == IS_DIRECT)
                .filter(cip -> cip.getCommentType().equals("comment"))
                .assignTimestampsAndWatermarks(new DateTimeAscendingAssigner())
                .map(new TwoHourMapFunction())
                .keyBy(new KeyByValue());

        DataStream<String> dayStream;
        DataStream<String> weekStream;
        DataStream<String> monthStream;


        dayStream = originalStream
                .timeWindow(Time.days(1))
                .aggregate(new CommentCounterAggregator(), new CommentCounterProcessWF())
                .keyBy(new KeyByWindowStart2())
                .timeWindow(Time.days(1))
                .apply(new ConcatBuilderWF());


        weekStream = originalStream
                .timeWindow(Time.days(7))
                .aggregate(new CommentCounterAggregator(), new CommentCounterProcessWF())
                .keyBy(new KeyByWindowStart2())
                .timeWindow(Time.days(7))
                .apply(new ConcatBuilderWF());

        monthStream = originalStream
                .window(new MonthWindowAssigner())
                .aggregate(new CommentCounterAggregator(), new CommentCounterProcessWF())
                .keyBy(new KeyByWindowStart2())
                .window(new MonthWindowAssigner())
                .apply(new ConcatBuilderWF());


        dayStream.writeAsText("/sabd/result/query2/24hour.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        weekStream.writeAsText("/sabd/result/query2/7days.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        monthStream.writeAsText("/sabd/result/query2/1month.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("Query2");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

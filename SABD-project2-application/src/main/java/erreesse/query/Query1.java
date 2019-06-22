package erreesse.query;

import erreesse.datasource.CommentInfoSource;
import erreesse.operators.aggregator.ArticleCounterAggregator;
import erreesse.operators.apply.RankingWF;
import erreesse.operators.keyby.KeyByArticleID;
import erreesse.operators.keyby.KeyByWindowStart;
import erreesse.operators.windowfunctions.ArticleCounterProcessWF;
import erreesse.pojo.CommentInfoPOJO;
import erreesse.time.CreateDateTimeAssigner;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Query1 {

    public static void main(String[] args) {

        // set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        KeyedStream<CommentInfoPOJO, String> originalStream = env
                .addSource(new CommentInfoSource())
                .map(line -> CommentInfoPOJO.parseFromStringLine(line))
                .assignTimestampsAndWatermarks(new CreateDateTimeAssigner())
                .keyBy(new KeyByArticleID());

        DataStream<String> hourStream;
        DataStream<String> dayStream;
        DataStream<String> weekStream;


        hourStream = originalStream
                .timeWindow(Time.hours(1))
                .aggregate(new ArticleCounterAggregator(), new ArticleCounterProcessWF())
                .keyBy(new KeyByWindowStart())
                .timeWindow(Time.hours(1))
                .apply(new RankingWF());

        dayStream = originalStream
                .timeWindow(Time.days(1))
                .aggregate(new ArticleCounterAggregator(), new ArticleCounterProcessWF())
                .keyBy(new KeyByWindowStart())
                .timeWindow(Time.days(1))
                .apply(new RankingWF());

        weekStream = originalStream
                .timeWindow(Time.days(7))
                .aggregate(new ArticleCounterAggregator(), new ArticleCounterProcessWF())
                .keyBy(new KeyByWindowStart())
                .timeWindow(Time.days(7))
                .apply(new RankingWF());

        hourStream.writeAsText("/sabd/result/query1/1hour.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        dayStream.writeAsText("/sabd/result/query1/1day.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        weekStream.writeAsText("/sabd/result/query1/1week.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("Test KafkaConnector");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

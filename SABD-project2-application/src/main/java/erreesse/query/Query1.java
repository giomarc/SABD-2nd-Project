package erreesse.query;

import erreesse.datasink.KafkaCommentInfoSink;
import erreesse.datasource.KafkaCommentInfoSource;
import erreesse.executionenvironment.RSExecutionEnvironment;
import erreesse.metrics.ProbabilisticLatencyAssigner;
import erreesse.operators.aggregator.ArticleCounterAggregator;
import erreesse.operators.apply.RankingWF;
import erreesse.operators.filter.CommentInfoPOJOValidator;
import erreesse.operators.keyby.KeyByArticleID;
import erreesse.operators.keyby.KeyByWindowStart;
import erreesse.operators.processwindowfunctions.ArticleCounterProcessWF;
import erreesse.pojo.CommentInfoPOJO;
import erreesse.time.DateTimeAscendingAssigner;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Query1 {

    public static void main(String[] args) {

        // set up environment
        StreamExecutionEnvironment env = RSExecutionEnvironment.getExecutionEnvironment().setParallelism(2);

        KeyedStream<CommentInfoPOJO, String> originalStream = env
                .addSource(new KafkaCommentInfoSource())
                .map(line -> CommentInfoPOJO.parseFromStringLine(line))
                .filter(new CommentInfoPOJOValidator())
                .map(new ProbabilisticLatencyAssigner())
                .assignTimestampsAndWatermarks(new DateTimeAscendingAssigner())
                //.assignTimestampsAndWatermarks(new DateTimeOutOfOrderAssigner())
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


        //For throughput compute only
        DataStreamSink<String> stringDataStreamSink = hourStream.union(dayStream, weekStream).addSink(new KafkaCommentInfoSink("query1-output-total"));


    /*    hourStream.addSink(new KafkaCommentInfoSink("query1-output-1hour"));
        dayStream.addSink(new KafkaCommentInfoSink("query1-output-1day"));
        weekStream.addSink(new KafkaCommentInfoSink("query1-output-1week")); */


        hourStream.writeAsText("/sabd/result/query1/1hour.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        dayStream.writeAsText("/sabd/result/query1/1day.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        weekStream.writeAsText("/sabd/result/query1/1week.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("Query1");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

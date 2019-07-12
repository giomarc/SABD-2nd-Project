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
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.CancellationException;

public class Query1Sliding {

    public static void main(String[] args) {

        // set up environment
        StreamExecutionEnvironment env = RSExecutionEnvironment.getExecutionEnvironment();

        KeyedStream<CommentInfoPOJO, String> originalStream = env
                // connect to Kafka consumer
                .addSource(new KafkaCommentInfoSource())
                // convert each string to POJO model
                .map(line -> CommentInfoPOJO.parseFromStringLine(line))
                // filter malformed POJOs
                .filter(new CommentInfoPOJOValidator())
                // enable latency tracking
                .map(new ProbabilisticLatencyAssigner())
                // extract and assing timestamp
                .assignTimestampsAndWatermarks(new DateTimeAscendingAssigner())
                // create a stream keyed by ArticleID
                .keyBy(new KeyByArticleID());

        DataStream<String> hourStream;
        DataStream<String> dayStream;
        DataStream<String> weekStream;


        hourStream = originalStream
                // group events in temporal window
                // using Sliding Window, window size = 1hour, window slide = 15 mins
                .timeWindow(Time.hours(1),Time.minutes(15))
                // when each element is added in window, use accumulator to increment the counter
                // where window triggers, invoke process window function to emit the counter result
                .aggregate(new ArticleCounterAggregator(), new ArticleCounterProcessWF())
                // create a keyed stream with key equals window start timestamp
                .keyBy(new KeyByWindowStart())
                // group events in temporal window
                .timeWindow(Time.hours(1),Time.minutes(15))
                // compute the ranking
                .apply(new RankingWF());

        dayStream = originalStream
                .timeWindow(Time.days(1),Time.minutes(15))
                .aggregate(new ArticleCounterAggregator(), new ArticleCounterProcessWF())
                .keyBy(new KeyByWindowStart())
                .timeWindow(Time.days(1),Time.minutes(15))
                .apply(new RankingWF());

        weekStream = originalStream
                .timeWindow(Time.days(7),Time.minutes(15))
                .aggregate(new ArticleCounterAggregator(), new ArticleCounterProcessWF())
                .keyBy(new KeyByWindowStart())
                .timeWindow(Time.days(7),Time.minutes(15))
                .apply(new RankingWF());


        //For throughput compute only
        DataStreamSink<String> stringDataStreamSink = hourStream.union(dayStream, weekStream).addSink(new KafkaCommentInfoSink("query1-output-total"));

        // write output query stream on plain text file
        hourStream.writeAsText("/sabd/result/query1/1hour.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        dayStream.writeAsText("/sabd/result/query1/1day.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        weekStream.writeAsText("/sabd/result/query1/1week.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("Query1");
        } catch (ProgramInvocationException | JobCancellationException | CancellationException e) {
            // catch cancelled hob by user
            System.err.println("Interrupted job by user");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}

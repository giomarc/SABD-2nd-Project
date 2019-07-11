package erreesse.query;

import erreesse.datasource.KafkaCommentInfoSource;
import erreesse.executionenvironment.RSExecutionEnvironment;
import erreesse.metrics.ProbabilisticLatencyAssigner;
import erreesse.operators.cogroup.ComputePopularUserCGF;
import erreesse.operators.filter.CommentInfoPOJOValidator;
import erreesse.operators.windowassigner.MonthWindowAssigner;
import erreesse.operators.windowfunctions.ComputeMostPopularUserWF;
import erreesse.pojo.CommentInfoPOJO;
import erreesse.time.DateTimeAscendingAssigner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.CancellationException;

public class Query3 {

    public static void main(String[] args) {


        // set up environment
        StreamExecutionEnvironment env = RSExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<CommentInfoPOJO> originalStream = env
                .addSource(new KafkaCommentInfoSource())
                .map(line -> CommentInfoPOJO.parseFromStringLine(line))
                .filter(new CommentInfoPOJOValidator())
                .map(new ProbabilisticLatencyAssigner())
                .assignTimestampsAndWatermarks(new DateTimeAscendingAssigner());


        SingleOutputStreamOperator<CommentInfoPOJO> directComment = originalStream.filter(cip -> cip.isDirect());
        SingleOutputStreamOperator<CommentInfoPOJO> indirectComment = originalStream.filter(cip -> !cip.isDirect());

        SingleOutputStreamOperator<String> hourStream;
        SingleOutputStreamOperator<String> weekStream;
        SingleOutputStreamOperator<String> monthStream;

        CoGroupedStreams<CommentInfoPOJO, CommentInfoPOJO>.Where<Long>.EqualTo cogroupedStreams =
                directComment.coGroup(indirectComment)
                .where((KeySelector<CommentInfoPOJO, Long>) commentInfoPOJO -> commentInfoPOJO.getCommentID())
                .equalTo((KeySelector<CommentInfoPOJO, Long>) commentInfoPOJO -> commentInfoPOJO.getInReplyTo());


        hourStream = cogroupedStreams
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new ComputePopularUserCGF())
                .timeWindowAll(Time.days(1))
                .apply(new ComputeMostPopularUserWF());

        weekStream = cogroupedStreams
                .window(TumblingEventTimeWindows.of(Time.days(7)))
                .apply(new ComputePopularUserCGF())
                .timeWindowAll(Time.days(7))
                .apply(new ComputeMostPopularUserWF());

        monthStream = cogroupedStreams
                .window(new MonthWindowAssigner())
                .apply(new ComputePopularUserCGF())
                .windowAll(new MonthWindowAssigner())
                .apply(new ComputeMostPopularUserWF());

        hourStream.writeAsText("/sabd/result/query3/1day.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        weekStream.writeAsText("/sabd/result/query3/1week.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        monthStream.writeAsText("/sabd/result/query3/1month.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        try {
            env.execute("Query3");
        } catch (ProgramInvocationException | JobCancellationException | CancellationException e) {
            System.err.println("Interrupted job by user");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}

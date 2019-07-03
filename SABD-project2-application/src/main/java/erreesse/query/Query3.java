package erreesse.query;

import erreesse.datasource.KafkaCommentInfoSource;
import erreesse.executionenvironment.RSExecutionEnvironment;
import erreesse.operators.cogroup.ComputePopularUserCGF;
import erreesse.operators.filter.CommentInfoPOJOValidator;
import erreesse.operators.windowassigner.MonthWindowAssigner;
import erreesse.operators.windowfunctions.ComputeMostPopularUserWF;
import erreesse.pojo.CommentInfoPOJO;
import erreesse.time.DateTimeAscendingAssigner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Query3 {

    public static void main(String[] args) {


        // set up environment
        StreamExecutionEnvironment env = RSExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<CommentInfoPOJO> originalStream = env
                .addSource(new KafkaCommentInfoSource())
                .map(line -> CommentInfoPOJO.parseFromStringLine(line))
                .filter(new CommentInfoPOJOValidator())
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


        /*cogroupedStreams
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .apply(new CoGroupFunction<CommentInfoPOJO, CommentInfoPOJO, String>() {
                    @Override
                    public void coGroup(Iterable<CommentInfoPOJO> iterableDirect,
                                        Iterable<CommentInfoPOJO> iterableIndirect,
                                        Collector<String> out) throws Exception {

                        StringBuilder sb = new StringBuilder();
                        double totalLike = 0.0;
                        double b = 0.0;


                        sb.append("iterableDirect:\n");
                        for (CommentInfoPOJO cip : iterableDirect) {
                            totalLike = totalLike + cip.getRecommendations();
                            sb.append(cip+"\n");
                        }
                        sb.append("iterableIndirect:\n");
                        for (CommentInfoPOJO cip : iterableIndirect) {
                            b = b + 1.0;
                            sb.append(cip+"\n");
                        }

                        double finalResult = 0.3 * totalLike + 0.7 * b;
                        sb.append("-------------CALCOLO LA POP-----------------------\n\n");
                        sb.append("a:"+totalLike+"; b:"+b+"; totale: "+finalResult);
                        sb.append("\n-------------------------------------------------------\n\n");
                        out.collect(sb.toString());

                    }
                }).writeAsText("/sabd/result/query3/test.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);*/

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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}

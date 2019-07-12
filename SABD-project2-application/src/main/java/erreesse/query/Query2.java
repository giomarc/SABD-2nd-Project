package erreesse.query;

import erreesse.datasource.KafkaCommentInfoSource;
import erreesse.executionenvironment.RSExecutionEnvironment;
import erreesse.metrics.LatencyTuple1;
import erreesse.metrics.ProbabilisticLatencyAssigner;
import erreesse.operators.aggregator.FasciaAggregator;
import erreesse.operators.filter.CommentInfoPOJOValidator;
import erreesse.operators.map.TwoHourMapFunction;
import erreesse.operators.processwindowfunctions.FasciaProcessWindowFunction;
import erreesse.operators.windowassigner.MonthWindowAssigner;
import erreesse.pojo.CommentInfoPOJO;
import erreesse.time.DateTimeAscendingAssigner;
import org.apache.flink.client.program.ProgramInvocationException;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.client.JobCancellationException;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.concurrent.CancellationException;

public class Query2 {

    public static void main(String[] args) {


        // set up environment
        StreamExecutionEnvironment env = RSExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<LatencyTuple1<Integer>> originalStream = env
                // connect to Kafka consumer
                .addSource(new KafkaCommentInfoSource())
                // convert each string to POJO model
                .map(line -> CommentInfoPOJO.parseFromStringLine(line))
                // filter malformed POJOs
                .filter(new CommentInfoPOJOValidator())
                // enable latency tracking
                .map(new ProbabilisticLatencyAssigner())
                // consider only direct comment
                .filter(cip -> cip.isDirect())
                // extract and assing timestamp
                .assignTimestampsAndWatermarks(new DateTimeAscendingAssigner())
                // create integer element mapped to event hour
                .map(new TwoHourMapFunction());

            // timeWindowAll per assegnare la finestra di 1 giorno / 7 giorni / 1 mese
            // NON POSSO USARE KEYBY altrimenti l'aggregatore incrementa solo la sua chiave
            // uso aggregatore custom (array con 12 posizioni)
            // per ogni elemento aggiunto alla finestra
            // determino la fascia
            // faccio hit ++ per ogni fascia
            // alla scadenza della finestra invoco la process window function
            // che ha ricevuto la mappa accumulatore dalla aggregate
            // a questo punto process window function emette una Tupla3<Timestamp, Stringa>
            // dove Stringa è count h00, count h02, ..., count h20, count h22

        DataStream<String> dayStream;
        DataStream<String> weekStream;
        DataStream<String> monthStream;

        dayStream = originalStream
                // group events in temporal window
                .timeWindowAll(Time.hours(24))
                // aggregate with custom accumulator
                // where window triggers, invoke process window function to emit the counter result
                .aggregate(new FasciaAggregator(), new FasciaProcessWindowFunction());

        weekStream = originalStream
                .timeWindowAll(Time.days(7))
                .aggregate(new FasciaAggregator(), new FasciaProcessWindowFunction());

        monthStream = originalStream
                .windowAll(new MonthWindowAssigner())
                .aggregate(new FasciaAggregator(), new FasciaProcessWindowFunction());

        // write output query stream on plain text file
        dayStream.writeAsText("/sabd/result/query2/24hour.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        weekStream.writeAsText("/sabd/result/query2/1week.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        monthStream.writeAsText("/sabd/result/query2/1month.txt", FileSystem.WriteMode.OVERWRITE).setParallelism(1);


        try {
            env.execute("Query2");
        } catch (ProgramInvocationException | JobCancellationException | CancellationException e) {
            System.err.println("Interrupted job by user");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }

}

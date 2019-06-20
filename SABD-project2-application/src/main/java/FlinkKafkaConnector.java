import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class FlinkKafkaConnector {

    public static void main(String[] args) {

        // set up environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //non obbligatorio
        //env.getConfig().setAutoWatermarkInterval(1000L);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("zookeeper.connect", "zookeeper:2181");
        properties.setProperty("group.id", "test");
        DataStream<String> stream = env
                .addSource(new FlinkKafkaConsumer<String>("query1", new SimpleStringSchema(), properties))
                .assignTimestampsAndWatermarks(new StringTimeAssigner())
                .map(e -> e.toUpperCase());

        stream.print();

        try {
            env.execute("Test KafkaConnector");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static class StringTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<String>{


        public StringTimeAssigner() {
            super(Time.seconds(5));
        }

        public long extractTimestamp(String s) {
            return System.currentTimeMillis();
        }
    }
}

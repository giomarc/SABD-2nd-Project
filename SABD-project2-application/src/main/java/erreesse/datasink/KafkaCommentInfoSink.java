package erreesse.datasink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaCommentInfoSink extends FlinkKafkaProducer<String> {

    public KafkaCommentInfoSink(String topicName) {
        super(topicName, new SimpleStringSchema(), initProperties());
        setWriteTimestampToKafka(true);

    }

    private static Properties initProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("zookeeper.connect", "zookeeper:2181");
        properties.setProperty("group.id", "test");
        return properties;
    }


}

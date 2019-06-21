package erreesse.datasource;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class CommentInfoSource extends FlinkKafkaConsumer<String> {

    public CommentInfoSource() {
        super("query1", new SimpleStringSchema(), initProperties());
    }

    private static Properties initProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "kafka:9092");
        properties.setProperty("zookeeper.connect", "zookeeper:2181");
        properties.setProperty("group.id", "test");
        return properties;
    }
}

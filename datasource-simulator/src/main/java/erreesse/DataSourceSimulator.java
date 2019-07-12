package erreesse;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class DataSourceSimulator {

    private String csvFilePath;

    private BufferedReader bufferedReader;

    private static final String kafkaUri = "kafka:9092";
    private static final String topicName = "query1";
    private KafkaProducer kafkaProducer;

    private float servingSpeed;


    public DataSourceSimulator(String aCsvFilePath, float aServingSpeed) {
        csvFilePath = aCsvFilePath;
        servingSpeed = aServingSpeed;

        initiCSVReader();
        initKafkaProducer();
    }

    private void initiCSVReader() {
        try {
            bufferedReader = new BufferedReader(new FileReader(csvFilePath));
        } catch (FileNotFoundException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }


    private void initKafkaProducer() {
        //For Kafkaproducer
        Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUri);
        props.put("group.id", "test");
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);
        kafkaProducer = new KafkaProducer(props);
    }

    private void sendToTopic(String value) {
        ProducerRecord record = new ProducerRecord(topicName,null,value);
        kafkaProducer.send(record);
    }

    public void startSimulation() {

        String firstLine = readLineFromCSV();
        long firstTimestamp = extractTimeStamp(firstLine);
        sendToTopic(firstLine);

        String line;
        while ((line = readLineFromCSV())!=null) {
            long curTimestamp = extractTimeStamp(line);

            long deltaTimeStamp = computeDelta(firstTimestamp,curTimestamp);

            //System.out.println("Waiting for "+deltaTimeStamp+" millisecs...\n");
            if (deltaTimeStamp > 0)
                addDelay(deltaTimeStamp);

            sendToTopic(line);

            firstTimestamp = curTimestamp;
        }

        String poisonedTuple = "1546300799,ffffffffffffffffffffffff,9999,9999,comment,1546300799,1,False,0,,0,Unknown,Unknown,9999,\"-\",,,,,,,,,,,,,,,,,,,";
        sendToTopic(poisonedTuple);

    }

    private void addDelay(long deltaTimeStamp) {
        try {
            Thread.sleep(deltaTimeStamp);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    private long computeDelta(long firstTimestamp, long curTimestamp) {
        long milliSecsDelta = (curTimestamp - firstTimestamp) * 1000L; // delta in millisecs
        return (long) (milliSecsDelta / servingSpeed);
    }


    private String readLineFromCSV() {
        String line = "";
        try {
            line = bufferedReader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return line;
    }

    private long extractTimeStamp(String line) {
        try {
            String[] tokens = line.split(",",-1);
            return Long.parseLong(tokens[5]);

        } catch (NumberFormatException e) {
            return 0L;
        }
    }

}

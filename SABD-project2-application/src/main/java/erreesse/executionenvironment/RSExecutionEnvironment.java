package erreesse.executionenvironment;

import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;

public class RSExecutionEnvironment {

    public static StreamExecutionEnvironment getExecutionEnvironment() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TIME CHARACTERISTIC
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        // STATEBACKEND and CHECKPOINTING
        //env.setStateBackend(new FsStateBackend("hdfs:///master:54310/flink-state/"));
        // start a checkpoint every 5000 ms
        //env.enableCheckpointing(5000);
        //env.getConfig().setLatencyTrackingInterval(5L);

        String filebackend = "alluxio://alluxio:19998/flink-state";
        try {
            env.setStateBackend(new RocksDBStateBackend(filebackend,true));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return env;
    }
}

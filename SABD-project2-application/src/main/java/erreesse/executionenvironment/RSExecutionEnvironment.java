package erreesse.executionenvironment;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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

        return env;
    }
}

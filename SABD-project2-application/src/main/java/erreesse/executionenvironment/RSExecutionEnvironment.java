package erreesse.executionenvironment;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class RSExecutionEnvironment {

    public static StreamExecutionEnvironment getExecutionEnvironment() {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // TIME CHARACTERISTIC
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // STATEBACKEND and CHECKPOINTING LOCAL FILESYSTEM
        // save checkpoints on local filesystem (shared volume with others containers)
        //env.setStateBackend(new FsStateBackend("file:///opt/checkpoint-flink"));

        // STATEBACKEND and CHECKPOINTING ALLUXIO
        String filebackend = "alluxio://alluxio-master:19998/flink-state";
        env.setStateBackend((StateBackend) new FsStateBackend(filebackend,true));

        // STATEBACKEND and CHECKPOINTING ROCKSDB
        /*try {
            env.setStateBackend(new RocksDBStateBackend("hdfs://master:54310/flink-state"));
        } catch (IOException e) {
            e.printStackTrace();
        }*/


        // start a checkpoint every 5000 ms
        env.enableCheckpointing(5000);

        return env;
    }
}

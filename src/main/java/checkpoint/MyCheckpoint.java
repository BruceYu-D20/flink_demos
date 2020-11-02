package yux.checkpoint;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class MyCheckpoint {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 单位毫秒
        env.enableCheckpointing(60000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        // checkpoint执行的时间，timeout；如果在60s内没有做完，会被停止
        checkpointConfig.setCheckpointTimeout(60000L);
        // 运行过程中，同一时间允许几个checkpoint，比如上一个checkpoint没做完，就算周期到了，第二个也不会执行
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        // 当上一个checkpoint之后，最少等待多久可以开始执行下一个checkpoint。即使周期到了，也不会执行
        checkpointConfig.setMinPauseBetweenCheckpoints(500);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        MemoryStateBackend msBackend = new MemoryStateBackend(10 * 1024 * 1024, true);
        FsStateBackend fsStateBackend = new FsStateBackend("", true);
        RocksDBStateBackend rocksDBStateBackend;
        try {
            rocksDBStateBackend = new RocksDBStateBackend("", true);
        } catch (IOException e) {
            e.printStackTrace();
        }

        env.setStateBackend(fsStateBackend);

        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));

    }
}

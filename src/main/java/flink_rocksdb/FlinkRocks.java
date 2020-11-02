package flink_rocksdb;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class FlinkRocks {

    /**
     * 在flink-conf.yaml中设置
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(150000);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(45000);

        SingleOutputStreamOperator<Integer> localhost = env.socketTextStream("master", 8899)
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.parseInt(value);
                    }
                });

        DataStream<Tuple2<Integer, String>> out = localhost.flatMap(new CountWithFunction());

        env.execute("execute flink rocks");


    }
}

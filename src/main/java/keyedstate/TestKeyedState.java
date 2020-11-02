package yux.keyedstate;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestKeyedState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<Long, Long>> source = env.fromElements(
                Tuple2.of(1L, 3L),
                Tuple2.of(2L, 4L),
                Tuple2.of(2L, 4L),
                Tuple2.of(2L, 20L),
                Tuple2.of(1L, 5L),
                Tuple2.of(1L, 8L),
                Tuple2.of(1L, 10L)
        );

        source.keyBy(0).flatMap(new KeyedCountState()).setParallelism(10).print();

        env.execute("test keyed state");
    }
}

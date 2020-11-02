package flink_test_dynamic_jar;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TestDynamic {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("master", 9988);

//        DataStream<Integer> out = source.map(new MapFunction<String, Integer>() {
//            @Override
//            public Integer map(String value) throws Exception {
//                MyTestFlinkDynamic testdy = new MyTestFlinkDynamic();
//                return testdy.dynamicAll(Integer.parseInt(value));
//            }
//        });
//
//        out.print();
//
//        env.execute("run dynamic");
    }
}

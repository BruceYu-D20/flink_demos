package flink_rocksdb.relationState;

import lombok.SneakyThrows;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.StreamContextEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * 错误代码演示
 * source和otherSource是两个源
 * 不能通过keyed state相互获取状态
 *
 * 两条路能获取互相状态：broadcast state，分布式缓存
 *
 */
public class UseOtherState {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
//        env.enableCheckpointing(60000);

        final MapStateDescriptor<Integer, Integer> mapDesc =
                new MapStateDescriptor<Integer, Integer>("mapdesc", Integer.class, Integer.class);

        // 模拟zc_cc数据 id,number 例如1，10
        DataStreamSource<String> source = env.socketTextStream("localhost", 9998);
        source.map(new MapFunction<String, Tuple2<Integer, Integer>>() {
            @Override
            public Tuple2<Integer, Integer> map(String value) throws Exception {
                String[] data = value.split(",");
                return new Tuple2<>(Integer.parseInt(data[0]), Integer.parseInt(data[1]));
            }
        }).keyBy(0)
            .map(new RichMapFunction<Tuple2<Integer,Integer>, Tuple2<Integer, Integer>>() {

                private MapState<Integer, Integer> mapState;

                @Override
                public void open(Configuration parameters) throws Exception {

                    mapState = getRuntimeContext().getMapState(mapDesc);
                }

                @Override
                public Tuple2<Integer, Integer> map(Tuple2<Integer, Integer> value) throws Exception {
                    Integer updatedValue = value.f1;
                    if(mapState.contains(value.f0)){
                        updatedValue += mapState.get(value.f0);
                    }
                    mapState.put(value.f0, updatedValue);
                    return new Tuple2<>(value.f0, updatedValue);
                }
            });

        DataStreamSource<String> otherSource = env.socketTextStream("localhost", 9999);
        otherSource
            .map(new MapFunction<String, Integer>() {
                @Override
                public Integer map(String value) throws Exception {
                    System.out.println("模拟下发数据： " + value);
                    return Integer.parseInt(value);
                }
            })
            .keyBy(new KeySelector<Integer, Integer>() {
                @Override
                public Integer getKey(Integer value) throws Exception {
                    return value;
                }
            })
            .process(new KeyedProcessFunction<Integer, Integer, String>() {
                @Override
                public void processElement(Integer key, Context ctx, Collector<String> out) throws Exception {
                    MapState<Integer, Integer> ccState = getRuntimeContext().getMapState(mapDesc);

                    if(ccState.contains(key)){
                        String processedState = key + "_______" + ccState.get(key);
                        System.out.println(processedState);
                        out.collect(processedState);
                    }
                }
            });

        env.execute("runjob");


    }
}

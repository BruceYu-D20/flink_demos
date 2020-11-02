package flink_pulsar_avro;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSource;
import org.apache.flink.streaming.connectors.pulsar.internal.AvroDeser;

import java.util.Properties;

public class FlinkPulsarConsume {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

//        Properties props = new Properties();
//        props.setProperty("topic", "test_avro_user_pojo");
//
//        FlinkPulsarSource<User> source = new FlinkPulsarSource<>(
//                "pulsar://172.16.108.110:6650",
//                "http://172.16.108.110:8080",
//                AvroDeser.of(User.class),
//                props);
//
//        DataStream<User> userStream = env.addSource(source);
//
//        DataStream<User> data = userStream.filter(new FilterFunction<User>() {
//            @Override
//            public boolean filter(User value) throws Exception {
//                return value.getName().equals("lisi");
//            }
//        });

//        data.print();

        env.execute("execute stream");

    }
}

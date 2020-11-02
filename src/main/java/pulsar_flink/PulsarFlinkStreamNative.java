package pulsar_flink;

import flink_pulsar_avro.UserPOJO;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.TopicKeyExtractor;
import org.apache.flink.streaming.connectors.pulsar.internal.AvroDeser;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarDeserializationSchemaWrapper;

import java.util.Optional;
import java.util.Properties;

public class PulsarFlinkStreamNative {

    public static void main(String[] args) throws Exception {


        String serverUrl = "pulsar://172.16.108.110:6650";
        String adminUrl = "http://172.16.108.110:8080";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);

//        DataStream<String> source = env.socketTextStream("localhost", 20527);
        DataStream<String> source = env.fromElements("aaaaa", "bbbbb");
        DataStream<UserPOJO> processed = source.map(new MapFunction<String, UserPOJO>() {
            @Override
            public UserPOJO map(String value) throws Exception {
                System.out.println(value);
                UserPOJO user = new UserPOJO();
                user.setName(value);
                user.setFavorite_number(1);
                user.setFavorite_color("red");
                return user;
            }
        });

        Properties props = new Properties();
        props.setProperty(PulsarOptions.FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
        props.setProperty(PulsarOptions.FAIL_ON_WRITE_OPTION_KEY, "true");
        props.setProperty(PulsarOptions.PULSAR_PRODUCER_OPTION_KEY_PREFIX + "producerName", "producer-2");
        props.setProperty(PulsarOptions.PULSAR_PRODUCER_OPTION_KEY_PREFIX + "sendTimeoutMs", "0");

        FlinkPulsarSink<UserPOJO> pSink = null;
        try{

            pSink =
                    new FlinkPulsarSink<UserPOJO>(
                            serverUrl,
                            adminUrl,
                            Optional.of("test_sink_dedump1"),
                            props,
                            TopicKeyExtractor.NULL,
                            UserPOJO.class);

        }catch (Exception e){
            e.printStackTrace();
        }

        processed.addSink(pSink);

        env.execute("do dedumplicate");
    }
}

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.pulsar.FlinkPulsarSink;
import org.apache.flink.streaming.connectors.pulsar.TopicKeyExtractor;
import org.apache.flink.streaming.connectors.pulsar.internal.PulsarOptions;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.common.schema.SchemaType;
import org.junit.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;

public class test1 {

    @Test
    public void testa(){
        for(int i = 0; i <= 100 ; i ++){
            int index = (int) (Math.random() * 3);
            System.out.println(index);
        }
    }

    @Test
    public void dedumpPulsar() throws Exception {
        String serverUrl = "pulsar://master:6650";
        String adminUrl = "http://master:8080";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(2000);
        DataStream<String> source = env.fromElements("aaaa", "bbbb", "aaaa");
        DataStream<String> processed = source.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println(value);
                return value;
            }
        });

        Properties props = new Properties();
        props.setProperty(PulsarOptions.FLUSH_ON_CHECKPOINT_OPTION_KEY, "true");
        props.setProperty(PulsarOptions.FAIL_ON_WRITE_OPTION_KEY, "true");
        props.setProperty(PulsarOptions.PULSAR_PRODUCER_OPTION_KEY_PREFIX + "producerName", "producer-1");
        props.setProperty(PulsarOptions.PULSAR_PRODUCER_OPTION_KEY_PREFIX + "sendTimeoutMs", "0");

        FlinkPulsarSink<String> pSink =
                new FlinkPulsarSink<String>(
                        serverUrl,
                        adminUrl,
                        Optional.of("test_sink_dedump"),
                        props,
                        TopicKeyExtractor.NULL,
                        null);

        processed.addSink(pSink);

        env.execute("do dedumplicate");






    }
}

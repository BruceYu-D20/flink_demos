package yux.pulsar;

import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pulsar.PulsarSourceBuilder;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;

import java.util.HashSet;
import java.util.Set;

public class FlinkPulsarConsumer {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);

        PulsarSourceBuilder<Row> build =
                PulsarSourceBuilder
                .builder(new AvroRowDeserializationSchema(Utils.PULSAR_HDFS_SCHEMA))
                .acknowledgementBatchSize(10)
                .acknowledgementBatchSize(100)
                .serviceUrl("pulsar://172.16.108.110:6650");

        Set<String> topics = new HashSet<>();
        topics.add("persistent://public/default/pulsar-hdfs-sink-topic");

        ConsumerConfigurationData<byte[]> consumerConfigurationData = new ConsumerConfigurationData<>();
        consumerConfigurationData.setSubscriptionType(SubscriptionType.Shared);
        consumerConfigurationData.setSubscriptionInitialPosition(SubscriptionInitialPosition.Latest);
        consumerConfigurationData.setTopicNames(topics);
        consumerConfigurationData.setSubscriptionName("my_subscrib2");

        build.pulsarAllConsumerConf(consumerConfigurationData);

        // build创建PulsarConsumerSource
        SourceFunction<Row> stream1 = build.build();

        DataStreamSource<Row> pulsarStream = env.addSource(stream1);

        pulsarStream.print();

        env.execute("pulsar test");

    }

}

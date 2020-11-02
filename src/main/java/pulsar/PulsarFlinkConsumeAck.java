package yux.pulsar;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.pulsar.PulsarSourceBuilder;
import org.apache.pulsar.client.api.PulsarClientException;

import javax.xml.transform.stream.StreamSource;

public class PulsarFlinkConsumeAck {

    public static void main(String[] args) throws Exception {

        PulsarSourceBuilder<String> builder =PulsarSourceBuilder
            .builder(new SimpleStringSchema())
            .acknowledgementBatchSize(1)
            .serviceUrl("pulsar://172.16.108.110:6650");

//        new PulsarConsumerSource

//        new StreamSource(builder.build());

    }
}

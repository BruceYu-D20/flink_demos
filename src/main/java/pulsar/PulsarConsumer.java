package yux.pulsar;

import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.GenericRecord;
import org.apache.pulsar.client.api.schema.GenericSchema;
import org.apache.pulsar.client.api.schema.RecordSchemaBuilder;
import org.apache.pulsar.client.api.schema.SchemaBuilder;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

public class PulsarConsumer {

    public static void main(String[] args) throws Exception {

        PulsarClient client = PulsarClient
                .builder()
                .serviceUrl("pulsar://172.16.108.110:6650")
                .build();

        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("my_record");
        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        schemaInfo.setSchema(Utils.PULSAR_HDFS_SCHEMA.getBytes());

        GenericSchema<GenericRecord> schema = Schema.generic(schemaInfo);

        Consumer<GenericRecord> consumer = client.newConsumer(schema)
            .topic("persistent://public/default/pulsar-hdfs-sink-topic")
            .subscriptionName("ms2")
            .subscriptionType(SubscriptionType.Shared)
            .subscribe();

        while(true){

            Message<GenericRecord> receive = consumer.receive();
            consumer.acknowledgeCumulative(receive);
            System.out.println(new String(receive.getData()));

        }

    }
}

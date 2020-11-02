package yux.pulsar;


import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

/**
 *
 */
public class PulsarProducder {

    public static void main(String[] args) throws Exception {
        RecordSchemaBuilder recordSchemaBuilder = SchemaBuilder.record("my_record");

        SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
        schemaInfo.setSchema(Utils.PULSAR_HDFS_SCHEMA.getBytes());

        GenericSchema<GenericRecord> schema = Schema.generic(schemaInfo);

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://172.16.108.110:6650")
                .build();

        Producer producer = client.newProducer(schema)
                .topic("persistent://public/default/pulsar-hdfs-sink-topic")
                .batchingMaxMessages(10)
                .create();

        String[] names = new String[]{"jack", "tom", "jerry"};
        Integer[] ages = new Integer[]{10, 18, 3};

        while(true){

            GenericRecordBuilder genericRecordBuilder = schema.newRecordBuilder();
            genericRecordBuilder.set("fname", names[(int)(Math.random() * 3)]);
            genericRecordBuilder.set("id", ages[(int)(Math.random() * 3)]);

            GenericRecord message = genericRecordBuilder.build();
            System.out.println(message);
            producer.sendAsync(message);

            Thread.sleep(200);
        }


    }
}

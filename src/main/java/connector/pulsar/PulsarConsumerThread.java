package connector.pulsar;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.data.RowData;
import org.apache.pulsar.client.api.*;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.UUID;

public class PulsarConsumerThread extends Thread {

    private final String serviceUrl;
    private final SubscriptionType subsType;
    private final String subsName;
    private final SubscriptionInitialPosition subPos;
    private final String topic;
    private final String tableSchame;
    private final DeserializationSchema<RowData> deserializer;
    private volatile boolean running;
    private volatile Consumer<byte[]> consumer;


    public PulsarConsumerThread(String serviceUrl,
                                SubscriptionType subsType,
                                String subsName,
                                SubscriptionInitialPosition subPos,
                                String topic,
                                String tableSchame,
                                DeserializationSchema<RowData> deserializer) {

        setDaemon(true);
        this.serviceUrl = serviceUrl;
        this.subsType = subsType;
        this.subsName = subsName;
        this.subPos = subPos;
        this.topic = topic;
        this.tableSchame = tableSchame;
        this.deserializer = deserializer;
        this.running = true;
    }

    @Override
    public void run() {

        if(!running){
            return;
        }

        try{
            PulsarClient client = PulsarClient.builder()
                    .serviceUrl(this.serviceUrl)
                    .build();

            this.consumer = client.newConsumer()
                    .topic(topic)
                    .subscriptionName(subsName)
                    .subscriptionInitialPosition(subPos)
                    .subscribe();

            RecordSchemaBuilder recordSchemaBuilder =
                    SchemaBuilder.record("schemaName_" + UUID.randomUUID().toString());
            SchemaInfo schemaInfo = recordSchemaBuilder.build(SchemaType.AVRO);
            schemaInfo.setSchema(tableSchame.getBytes());
            GenericSchema<GenericRecord> schema = Schema.generic(schemaInfo);

            while(running){
                byte[] msg = this.consumer.receive().getData();
                RowData rowData = this.deserializer.deserialize(msg);
            }

        }catch(Exception e){
            e.printStackTrace();
        }
    }
}

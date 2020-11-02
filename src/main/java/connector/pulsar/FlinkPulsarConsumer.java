package connector.pulsar;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

public class FlinkPulsarConsumer extends RichParallelSourceFunction<RowData> implements ResultTypeQueryable<RowData> {

    protected final String serviceUrl;
    protected final SubscriptionType subsType;
    protected final String subsName;
    protected final SubscriptionInitialPosition subPos;
    protected final String topic;
    protected final String tableSchame;
    protected final DeserializationSchema<RowData> deserializer;

    public FlinkPulsarConsumer(String serviceUrl,
                               SubscriptionType subsType,
                               String subsName,
                               SubscriptionInitialPosition subPos,
                               String topic,
                               String tableSchema,
                               DeserializationSchema<RowData> deserializer) {
        this.serviceUrl = serviceUrl;
        this.subsType = subsType;
        this.subsName = subsName;
        this.subPos = subPos;
        this.topic = topic;
        this.tableSchame = tableSchema;
        this.deserializer = deserializer;
    }

    @Override
    public void open(Configuration parameters) throws Exception {

        this.deserializer.open(() -> getRuntimeContext().getMetricGroup().addGroup("user"));

    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.deserializer.getProducedType();
    }

    @Override
    public void run(SourceContext<RowData> ctx) throws Exception {


        new PulsarConsumerThread(serviceUrl, subsType, subsName, subPos, topic, tableSchame, deserializer);



    }

    @Override
    public void cancel() {

    }
}

package connector.pulsar;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

public class PulsarDynamicTableSource implements ScanTableSource {

    protected final String serviceUrl;
    protected final SubscriptionType subsType;
    protected final String subsName;
    protected final SubscriptionInitialPosition subPos;
    protected final String topic;
    protected final String tableschema;
    protected final DecodingFormat<DeserializationSchema<RowData>> decodingFormat; ;
    protected final DataType dataType;


    public PulsarDynamicTableSource(
            String serviceUrl,
            SubscriptionType subsType,
            String subsName,
            SubscriptionInitialPosition subPos,
            String topic,
            String tableschema,
            DecodingFormat<DeserializationSchema<RowData>> decodingFormat,
            DataType dataType) {

        Preconditions.checkNotNull(serviceUrl, "");
        Preconditions.checkNotNull(subsType, "");
        Preconditions.checkNotNull(subsName, "");
        Preconditions.checkNotNull(subPos, "");
        Preconditions.checkNotNull(subsName, "");
        Preconditions.checkNotNull(decodingFormat, "");
        Preconditions.checkNotNull(dataType, "");
        this.serviceUrl = serviceUrl;
        this.subsType = subsType;
        this.subsName = subsName;
        this.subPos = subPos;
        this.topic = topic;
        this.tableschema = tableschema;
        this.decodingFormat = decodingFormat;
        this.dataType = dataType;
    }

    @Override
    public DynamicTableSource copy() {
        return new PulsarDynamicTableSource(serviceUrl, subsType, subsName, subPos, topic, tableschema, decodingFormat, dataType);
    }

    @Override
    public String asSummaryString() {
        return "pulsar dynamic table";
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return this.decodingFormat.getChangelogMode();
    }

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
        DeserializationSchema<RowData> deserializationSchema =
                this.decodingFormat.createRuntimeDecoder(runtimeProviderContext, this.dataType);
        FlinkPulsarConsumer flinkPulsarConsumer =
                new FlinkPulsarConsumer(serviceUrl, subsType, subsName, subPos, topic, tableschema, deserializationSchema);
        return SourceFunctionProvider.of(flinkPulsarConsumer, false);
    }
}

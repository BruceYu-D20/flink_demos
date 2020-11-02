package connector.pulsar;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.Set;

public class PulsarDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "pulsar";

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        ReadableConfig options = helper.getOptions();
        String serviceUrl = options.get(PulsarOptions.SERVICEURL);
        String subsType = options.get(PulsarOptions.SUBSCRIPTIONTYPE);
        // parsed
        SubscriptionType subsTypeParsed = parseSubscriptionType(subsType);
        String subsName = options.get(PulsarOptions.SUBSCRIPTIONNAME);
        String subPos = options.get(PulsarOptions.SUBSCRIPTIONINITIALPOS);
        // parsed
        SubscriptionInitialPosition subPosParsed = parseSubscriptionInitialPosition(subPos);
        String topic = options.get(PulsarOptions.TOPIC);
        String tableschema = options.get(PulsarOptions.TABLESCHEMA);
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        DataType dataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        return new PulsarDynamicTableSource(serviceUrl, subsTypeParsed, subsName, subPosParsed, topic, tableschema, decodingFormat, dataType);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return null;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return null;
    }

    private static SubscriptionType parseSubscriptionType(String subscriptionType){

        if(subscriptionType.trim().toLowerCase().equals("exclusive")){
            return SubscriptionType.Exclusive;
        }else if(subscriptionType.trim().toLowerCase().equals("keyshared")){
            return SubscriptionType.Key_Shared;
        }else if(subscriptionType.trim().toLowerCase().equals("failover")){
            return SubscriptionType.Failover;
        }else {
            return SubscriptionType.Shared;
        }
    }

    private static SubscriptionInitialPosition parseSubscriptionInitialPosition(String subscriptionInitialPosition){

        if(subscriptionInitialPosition.trim().toLowerCase().equals("latest")){
            return SubscriptionInitialPosition.Latest;
        }else{
            return SubscriptionInitialPosition.Earliest;
        }
    }
}

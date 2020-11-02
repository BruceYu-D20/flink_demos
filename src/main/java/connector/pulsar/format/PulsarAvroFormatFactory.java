package connector.pulsar.format;

import connector.pulsar.PulsarOptions;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class PulsarAvroFormatFactory implements DeserializationFormatFactory {

    public static final ConfigOption<String> tableSchema = PulsarOptions.TABLESCHEMA;
    public static final ConfigOption<String> schemaName = ConfigOptions
            .key("schema_name")
            .stringType()
            .noDefaultValue()
            .withDescription("required url for pulsar");


    public static String IDENTIFIER = "pulsar_avro";
    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {
        String tableSchemaStr = formatOptions.get(tableSchema);
        String schemaNameStrr = formatOptions.get(schemaName);
        return new PulsarAvroFormat(tableSchemaStr, schemaNameStrr);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOps = new HashSet<>();
        requiredOps.add(tableSchema);
        requiredOps.add(schemaName);
        return requiredOps;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}

package connector.socket.format;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class ChangelogCsvFormatFactory implements DeserializationFormatFactory {

    public static final String IDENTIFIER = "changelog-csv";

    final ConfigOption<String> COLUMNDELIMITER = ConfigOptions.key("column_delimiter")
            .stringType()
            .defaultValue("|");

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);
        final String columnDelimiter = formatOptions.get(COLUMNDELIMITER);
        return new ChangelogCsvFormat(columnDelimiter);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOps = new HashSet<>();
        optionalOps.add(COLUMNDELIMITER);
        return optionalOps;
    }
}

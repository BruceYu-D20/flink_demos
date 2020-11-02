package connector.socket;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;


/**
 *
 * CREATE TABLE UserScores (name STRING, score INT)
     WITH (
     'connector' = 'socket',
     'hostname' = 'localhost',
     'port' = '9999',
     'byte-delimiter' = '10',
     'format' = 'changelog-csv',
     'changelog-csv.column-delimiter' = '|'
     );
 *
 */
public class SocketDynamicTableFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "socket";

    public static final ConfigOption<String> HOSTNAME = ConfigOptions
            .key("hostname")
            .stringType()
            .noDefaultValue();

    public static final ConfigOption<Integer> PORT = ConfigOptions
            .key("port")
            .intType()
            .noDefaultValue();

    // 每一行的间隔字符
    public static final ConfigOption<Integer> BYTE_DELEMITER = ConfigOptions
            .key("byte-delimiter")
            .intType()
            .defaultValue(10);

    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {

        FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
        DecodingFormat<DeserializationSchema<RowData>> decodingFormat =
                helper.discoverDecodingFormat(DeserializationFormatFactory.class, FactoryUtil.FORMAT);
        ReadableConfig options = helper.getOptions();
        final String hostname = options.get(HOSTNAME);
        final Integer port = options.get(PORT);
        final byte byteDelemiter = (byte) (int) options.get(BYTE_DELEMITER);

        DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();
        return new SocketDynamicTableSource(hostname, port, byteDelemiter, decodingFormat, producedDataType);
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        Set<ConfigOption<?>> requiredOps = new HashSet<>();
        requiredOps.add(HOSTNAME);
        requiredOps.add(PORT);
        requiredOps.add(FactoryUtil.FORMAT);
        return requiredOps;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> optionalOps = new HashSet<>();
        optionalOps.add(BYTE_DELEMITER);
        return optionalOps;
    }
}

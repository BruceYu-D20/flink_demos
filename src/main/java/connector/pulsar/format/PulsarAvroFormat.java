package connector.pulsar.format;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.RowKind;

import java.util.List;

public class PulsarAvroFormat implements DecodingFormat<DeserializationSchema<RowData>> {

    private final String tableSchema;
    private final String schemaName;

    public PulsarAvroFormat(String tableSchema, String schemaName) {
        this.tableSchema = tableSchema;
        this.schemaName = schemaName;
    }

    @Override
    public DeserializationSchema<RowData> createRuntimeDecoder(
            DynamicTableSource.Context context,
            DataType producedDataType) {

        TypeInformation<RowData> typeInformation =
                (TypeInformation<RowData>) context.createTypeInformation(producedDataType);
        DynamicTableSource.DataStructureConverter converter =
                context.createDataStructureConverter(producedDataType);
        List<LogicalType> parsingTypes =
                producedDataType.getLogicalType().getChildren();
        return new PulsarAvroDeserializer(typeInformation, converter, parsingTypes,
                this.tableSchema, this.schemaName);
    }

    @Override
    public ChangelogMode getChangelogMode() {
        return ChangelogMode.newBuilder()
                .addContainedKind(RowKind.INSERT)
                .addContainedKind(RowKind.INSERT)
                .build();
    }
}

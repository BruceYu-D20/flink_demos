package connector.pulsar.format;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.util.List;

public class PulsarAvroDeserializer implements DeserializationSchema<RowData> {

    private final TypeInformation<RowData> typeInformation;
    private final DynamicTableSource.DataStructureConverter converter;
    private final List<LogicalType> parsingTypes;
    private final String tableSchema;
    private final String schemaName;

    public PulsarAvroDeserializer(
            TypeInformation<RowData> typeInformation,
            DynamicTableSource.DataStructureConverter converter,
            List<LogicalType> parsingTypes,
            String tableSchema,
            String schemaName) {
        this.typeInformation = typeInformation;
        this. converter = converter;
        this.parsingTypes = parsingTypes;
        this.tableSchema = tableSchema;
        this.schemaName = schemaName;
    }

    @Override
    public RowData deserialize(byte[] message) throws IOException {

        RecordSchemaBuilder recordBuilder = SchemaBuilder.record(schemaName);
        SchemaInfo schemaInfo = recordBuilder.build(SchemaType.AVRO);
        schemaInfo.setSchema(this.tableSchema.getBytes());
        GenericSchema<GenericRecord> schema = Schema.generic(schemaInfo);

        GenericRecord genericRecord = schema.decode(message);
        List<Field> fields = genericRecord.getFields();
        Row row = new Row(fields.size());
        for(int i = 0; i < fields.size(); i ++){
            row.setField(i, fields.get(i));
        }

        return (RowData) converter.toInternal(row);
    }

    @Override
    public boolean isEndOfStream(RowData nextElement) {
        return false;
    }

    @Override
    public TypeInformation<RowData> getProducedType() {
        return this.typeInformation;
    }
}

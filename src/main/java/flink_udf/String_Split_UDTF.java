package flink_udf;

import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

public class String_Split_UDTF extends TableFunction<Row>{

    public void eval(String value1){

        byte b = value1.getBytes()[2];
        byte[] split = {b};
        String splitChar = new String(split);

        Row row = new Row(2);
        row.setField(0, value1.split(splitChar)[0]);
        row.setField(1, value1.split(splitChar)[1]);
        collect(row);
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.STRING, Types.STRING);
    }
}

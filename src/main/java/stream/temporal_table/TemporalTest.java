package stream.temporal_table;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.$;

public class TemporalTest extends AbstractTestBase {

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tenv;

    @Before
    public void createEnvs(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(10);
        EnvironmentSettings envSetting =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tenv = StreamTableEnvironment.create(env, envSetting);
    }

    @Test
    public void test1() throws Exception {
        DataStreamSource<Tuple3<String, String, Integer>> stream = env.fromElements(
                new Tuple3<String, String, Integer>("09:00:00", "US Dollar", 112),
                new Tuple3<String, String, Integer>("09:00:00", "Euro", 114),
                new Tuple3<String, String, Integer>("09:00:00", "Yen", 1),
                new Tuple3<String, String, Integer>("10:45:00", "Euro", 116),
                new Tuple3<String, String, Integer>("11:15:00", "Euro", 119),
                new Tuple3<String, String, Integer>("11:49:00", "Pounds", 108)
        );

        Table table = tenv.fromDataStream(stream, $("rowtime"), $("currency"), $("tmp"));
        tenv.createTemporaryView("RatesHistory", table);

        String sql =
            "SELECT *" +
            "  FROM RatesHistory AS r" +
            "  WHERE r.rowtime = (" +
            "  SELECT MAX(rowtime)" +
            "  FROM RatesHistory AS r2" +
            "  WHERE r2.currency = r.currency" +
            "  AND r2.rowtime <= TIME '10:58:00')";

        Table result = tenv.sqlQuery(sql);
        tenv.createTemporaryView("my_catalog.other_database.result_table", result);

//        DataStream<Tuple2<Boolean, Tuple3<String, String, Integer>>> dataStream2 =
//                tenv.toRetractStream(result, TypeInformation.of(new TypeHint<Tuple3<String, String, Integer>>() {
//        }));
//        dataStream2.print();

        DataStream<Tuple3<String, String, Integer>> a = tenv.toAppendStream(result, TypeInformation.of(new TypeHint<Tuple3<String, String, Integer>>() {
        }));
        a.print();
    }
}

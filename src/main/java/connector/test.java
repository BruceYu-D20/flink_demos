package connector;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.table.api.Expressions.$;

public class test extends AbstractTestBase {

    StreamExecutionEnvironment senv;
    ExecutionEnvironment env;
    StreamTableEnvironment tableEnv;


    @Before
    public void createExecuteEnv(){

        senv = StreamExecutionEnvironment.getExecutionEnvironment();
        env = ExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tableEnv = StreamTableEnvironment.create(senv, bsSettings);
    }

    @Test
    public void test1(){
        String socketSql =
                "CREATE TABLE UserScores (name STRING, score INT)" +
                "WITH (" +
                "  'connector' = 'socket'," +
                "  'hostname' = 'localhost'," +
                "  'port' = '9999'," +
                "  'byte-delimiter' = '10'," +
                "  'format' = 'changelog-csv'," +
                "  'changelog-csv.column-delimiter' = '|'" +
                ")";

        tableEnv.executeSql(socketSql);

        String sqlCompute = "SELECT name, SUM(score) FROM UserScores GROUP BY name";
        Table table = tableEnv.sqlQuery(sqlCompute);
        TypeInformation<Tuple2<String, Integer>> typeInformation =
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {});
        DataStream<Tuple2<Boolean, Tuple2<String, Integer>>> ds = tableEnv.toRetractStream(table, typeInformation);
        DataStream<String> result = ds.map(new MapFunction<Tuple2<Boolean, Tuple2<String, Integer>>, String>() {
            @Override
            public String map(Tuple2<Boolean, Tuple2<String, Integer>> value) throws Exception {
                return value.f1.f0 + "_" + value.f1.f1;
            }
        });
        result.print();

        tableEnv.fromDataStream(result, $("name"), $("time").proctime());

    }

    @After
    public void end() throws Exception {
        senv.execute("run job");
    }
}

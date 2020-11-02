package table_sql;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.StatementSet;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


import static org.apache.flink.table.api.Expressions.$;

public class Demos extends AbstractTestBase {

    StreamExecutionEnvironment senv;
    ExecutionEnvironment env;
    StreamTableEnvironment bsTableEnv;

    @Before
    public void createExecuteEnv(){

        senv = StreamExecutionEnvironment.getExecutionEnvironment();
        env = ExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        bsTableEnv = StreamTableEnvironment.create(senv, bsSettings);
    }

    @Test
    public void dataStreamTuple2Table(){

        DataStream<Tuple2<String, String>> source = senv.socketTextStream("localhost", 33771)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> map(String s) throws Exception {
                        return Tuple2.of(s.split(",")[0], s.split(",")[1]);
                    }
                });
        bsTableEnv.createTemporaryView("ds2tb", source, $("id"), $("name"));

    }

    @Test
    public void DataStreamPOJO2Table(){
        DataStream<StudentPojo> source = senv.socketTextStream("localhost", 33771)
            .map(new MapFunction<String, StudentPojo>() {
                @Override
                public StudentPojo map(String s) throws Exception {
                    return new StudentPojo(Integer.parseInt(s.split(",")[0]), s.split(",")[1]);
                }
            });
        Table table = bsTableEnv.fromDataStream(source, $("id"), $("name"));
        bsTableEnv.createTemporaryView("my_view", table);

        Table signedInfoTable = bsTableEnv.sqlQuery("SELECT id,name,'1' as sign FROM my_view");

        signedInfoTable.printSchema();
        signedInfoTable.execute().print();
    }

    @Test
    public void tableConnectorAnd2DataStream(){
        String connectorSQL =
                "CREATE TABLE MY_VIEW_TABLE(" +
                    "id int," +
                    "name string," +
                    "age int," +
                    "money int" +
                ") WITH (" +
                    "'connector.type' = 'jdbc'," +
                    "'connector.url' = 'jdbc:mysql://localhost:3306/test'," +
                    "'connector.table' = 'account'," +
                    "'connector.driver' = 'com.mysql.jdbc.Driver'," +
                    "'connector.username' = 'root'," +
                    "'connector.password' = 'root'," +
                    "'connector.read.query' = 'SELECT id,name,age,money FROM account'" +
                ")";

        bsTableEnv.executeSql(connectorSQL);
        Table connectorTb = bsTableEnv.sqlQuery("SELECT * FROM MY_VIEW_TABLE");
        TypeInformation<Tuple4<Integer, String, Integer, Integer>> types =
                TypeInformation.of(new TypeHint<Tuple4<Integer, String, Integer, Integer>>() {
        });
        DataStream<Tuple4<Integer, String, Integer, Integer>> ds = bsTableEnv.toAppendStream(connectorTb, types);
        DataStream<String> resultDs = ds.map(new MapFunction<Tuple4<Integer, String, Integer, Integer>, String>() {
            @Override
            public String map(Tuple4<Integer, String, Integer, Integer> value) throws Exception {
                return value.f0 + "_" + value.f1 + "_" + value.f2 + "_" + value.f3;
            }
        });
        resultDs.print();
    }

    @Test
    public void testTableApi() throws Exception {
        String connectorSQL =
                "CREATE TABLE MY_VIEW_TABLE(" +
                        "id int," +
                        "name string," +
                        "age int," +
                        "money int" +
                        ") WITH (" +
                        "'connector.type' = 'jdbc'," +
                        "'connector.url' = 'jdbc:mysql://localhost:3306/test'," +
                        "'connector.table' = 'account'," +
                        "'connector.driver' = 'com.mysql.jdbc.Driver'," +
                        "'connector.username' = 'root'," +
                        "'connector.password' = 'root'," +
                        "'connector.read.query' = 'SELECT id,name,age,money FROM account'" +
                        ")";

        bsTableEnv.executeSql(connectorSQL);
        Table my_view_table = bsTableEnv.from("MY_VIEW_TABLE");
        // test table api
        Table tableApiTable = my_view_table.filter($("age").isLessOrEqual(30))
                .groupBy($("name"), $("age"))
                .select($("name"), $("age"), $("money").sum().as("totalMoney"));


//        bsTableEnv.toRetractStream(tableApiTable, Row.class).print("#####");
        bsTableEnv.createTemporaryView("print_view", tableApiTable);
        String printSQL =
            "CREATE TABLE sink_print(" +
                "name string," +
                "age int," +
                "total_money int" +
                ") " +
            "WITH " +
            "(" +
                "'connector'='print'," +
                "'connector.read.query'='SELECT name,age,totalMoney FROM print_view'" +
            ")";
//        String insertSQL = "INSERT INTO sink_print SELECT * FROM print_view";
//        bsTableEnv.executeSql(insertSQL);
//        StatementSet stmt = bsTableEnv.createStatementSet();
//        stmt.addInsert("sink_print", tableApiTable);
        bsTableEnv.executeSql(printSQL);

    }

    @After
    public void execute() throws Exception {
//        senv.execute("run job");
    }
}

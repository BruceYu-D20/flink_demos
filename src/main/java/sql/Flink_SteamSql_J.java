package sql;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class Flink_SteamSql_J {

    public static void main(String[] args) throws Exception {

        // 1. 创建env
        // ***************
        // STREAMING QUERY
        // ***************
        StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment stenv = StreamTableEnvironment.create(senv);

        DataStream<String> X = senv.fromElements("a", "b", "c");

        stenv.registerDataStream("myX", X, "name");

        Table xInfo = stenv.sqlQuery("SELECT * FROM myX");

        xInfo.printSchema();


        senv.execute();



    }
}

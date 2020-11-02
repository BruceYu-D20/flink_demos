package yux.broadcast.UserPurchaseBehavior.test;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Test;
import yux.broadcast.UserPurchaseBehavior.MysqlConfSource;
import yux.broadcast.UserPurchaseBehavior.pojo.Config;
import yux.broadcast.mysqlConfBst.MysqlSource;

import java.util.HashMap;

public class PurchaseTest extends AbstractTestBase {

    @Test
    public void testMysqlSource() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/test";
        String user = "root";
        String passwd = "root";
        Integer secondInterval = 5;

        DataStreamSource<HashMap<String, Config>> mysqlSource =
                env.addSource(new MysqlConfSource(jdbcUrl, user, passwd, secondInterval)).setParallelism(1);

        mysqlSource.print();


        env.execute("test mysql source");
    }
}

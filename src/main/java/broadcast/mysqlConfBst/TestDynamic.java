package broadcast.mysqlConfBst;

import broadcast.mysqlConfBst.MysqlSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.junit.Test;
import java.util.HashMap;
import java.util.Map;

public class TestDynamic extends AbstractTestBase {

    @Test
    public void testMysqlSource() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/test";
        String user = "root";
        String passwd = "root";
        Integer secondInterval = 10;

        DataStreamSource<HashMap<String, Tuple2<String, Integer>>> mysqlSource =
                env.addSource(new MysqlSource(jdbcUrl, user, passwd, secondInterval));

        mysqlSource.map(new MapFunction<HashMap<String,Tuple2<String,Integer>>, String>() {
            @Override
            public String map(HashMap<String, Tuple2<String, Integer>> value) throws Exception {

                System.out.println(System.currentTimeMillis());
                for(String key: value.keySet()){
                    System.out.println(key + "," + value.get(key));
                }
                return null;
            }
        });


        env.execute("test mysql source");

    }

    @Test
    public void testBroadcastState() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/test";
        String user = "root";
        String passwd = "root";
        Integer secondInterval = 10;

        // timestamp,id
        DataStreamSource<String> customInStream = env.socketTextStream("localhost", 9999);

        // custom data Tuple2<Long, String>
        SingleOutputStreamOperator<Tuple2<Long, String>> customProcessStream =
            customInStream.process(new ProcessFunction<String, Tuple2<Long, String>>() {
                @Override
                public void processElement(String customData, Context ctx,
                                           Collector<Tuple2<Long, String>> out) throws Exception {

                    String[] splited = customData.split(",");
                    out.collect(new Tuple2<Long, String>(Long.parseLong(splited[0]), splited[1]));
                }
            });

        // mysql data Tuple2<String, Integer>
        DataStreamSource<HashMap<String, Tuple2<String, Integer>>> mysqlStream =
                env.addSource(new MysqlSource());
        mysqlStream.print();

        /*
          (1) 先建立MapStateDescriptor
          MapStateDescriptor定义了状态的名称、Key和Value的类型。
          这里，MapStateDescriptor中，key是Void类型，value是Map<String, Tuple2<String,Integer>>类型。

          是mysql data的类型
         */
        MapStateDescriptor<Void, Map<String, Tuple2<String, Integer>>> configDescriptor =
                new MapStateDescriptor<>("config", Types.VOID,
                        Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));

        /*
          (2) 将mysql的数据流广播，形成BroadcastStream，并添加上mysql data的类型
         */
        BroadcastStream<HashMap<String, Tuple2<String, Integer>>> broadcastConfigStream =
                mysqlStream.broadcast(configDescriptor);

        //(3)事件流和广播的配置流连接，形成BroadcastConnectedStream
        BroadcastConnectedStream<Tuple2<Long, String>, HashMap<String, Tuple2<String, Integer>>> connectedStream =
                customProcessStream.connect(broadcastConfigStream);

        //(4)对BroadcastConnectedStream应用process方法，根据配置(规则)处理事件
        SingleOutputStreamOperator<Tuple4<Long, String, String, Integer>> resultStream =
                connectedStream.process(new CustomBroadcastProcessFunction(configDescriptor));

        resultStream.print();

        env.execute("testBroadcastState");

    }


}

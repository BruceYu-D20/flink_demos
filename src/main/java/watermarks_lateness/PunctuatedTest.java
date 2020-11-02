package watermarks_lateness;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.junit.Test;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

/**
 * flink 的测试类：
 * 1. 继承 AbstractTestBase
 * 2. 每个方法上标注上@Test
 */


public class PunctuatedTest extends AbstractTestBase {

    @Test
    public void testMyWatermarkerAssigner() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(200);

        DataStream<String> instream = env.socketTextStream("localhost", 9000, "\n");

        SingleOutputStreamOperator<String> process =
            instream
                .map(new MapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(String s) throws Exception {

                        String[] splited = s.split(",");
                        return new Tuple2<Long, String>(Long.parseLong(splited[0]), splited[1]);
                    }
                })
                .assignTimestampsAndWatermarks(new MyWatermarkerAssigner())
                .keyBy(new KeySelector<Tuple2<Long,String>, String>() {

                    @Override
                    public String getKey(Tuple2<Long, String> longStringTuple2) throws Exception {
                        return longStringTuple2.f1;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<Long, String>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<Long, String>> elements,
                                        Collector<String> out) throws Exception {
                        Long waterMark = context.currentWatermark();
                        Long winStart = context.window().getStart();
                        Long winEnd = context.window().getEnd();

                        Iterator<Tuple2<Long, String>> itr = elements.iterator();
                        while (itr.hasNext()){
                            String info = "waterMark: " + waterMark + " winStart: " + winStart +
                                    " winEnd: " + winEnd + itr.next();
                            out.collect(info);
                        }
                    }
                });

        process.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {

                System.out.println(s);
                System.out.println();
                return s;
            }
        });


        env.execute("test my watermark");
    }

    /**
     * c3p0 flink 异步连接mysql
     *
     * @throws Exception
     */
    @Test
    public void testAsynMysql() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 时间戳,
        DataStream<String> instream = env.socketTextStream("localhost", 9000, "\n");

        DataStream<String> process =
            instream
                .map(new MapFunction<String, Tuple2<Long, String>>() {
                    @Override
                    public Tuple2<Long, String> map(String s) throws Exception {

                        String[] splited = s.split(",");
                        return new Tuple2<Long, String>(Long.parseLong(splited[0]), splited[1]);
                    }
                })
                .assignTimestampsAndWatermarks(new MyWatermarkerAssigner())
                .keyBy(new KeySelector<Tuple2<Long,String>, String>() {

                    @Override
                    public String getKey(Tuple2<Long, String> longStringTuple2) throws Exception {
                        return longStringTuple2.f1;
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Tuple2<Long, String>, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<Long, String>> elements,
                                        Collector<String> out) throws Exception {

                    Iterator<Tuple2<Long, String>> itr = elements.iterator();
                    while (itr.hasNext()){
                        out.collect(itr.next().f1);
                    }
                    }
                });

        DataStream<String> asynprocess =
                AsyncDataStream.unorderedWait(process, new MysqlAsynFunctionFuture(),
                    10000, TimeUnit.MILLISECONDS, 100);

        DataStream<String> result = asynprocess.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value;
            }
        });

        result.print();

        env.execute("test asyn");

    }
}

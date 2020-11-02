package stream.table_window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.GroupWindow;
import org.apache.flink.table.api.*;
import static org.apache.flink.table.api.Expressions.*;

import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;

import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;


public class Demos extends AbstractTestBase {

    StreamExecutionEnvironment senv;
    ExecutionEnvironment env;
    StreamTableEnvironment tenv;

    @Before
    public void createExecuteEnv(){

        senv = StreamExecutionEnvironment.getExecutionEnvironment();
        env = ExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        tenv = StreamTableEnvironment.create(senv, bsSettings);
    }

    @Test
    public void testGroupWindow(){
        DataStreamSource<String> datasource = senv.socketTextStream("localhost", 9999);
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<Tuple3<Long, String, Integer>> data1 = datasource.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String value) throws Exception {
                Long currentTime = System.currentTimeMillis() / 1000;
                String name = value.split(",")[0];
                Integer money = Integer.parseInt(value.split(",")[1]);
                return new Tuple3<>(currentTime, name, money);
            }
        }).assignTimestampsAndWatermarks(new TestPeriodicWatermarkerAssigner());

        Table table1 = tenv.fromDataStream(data1, $("rowtime"), $("name"), $("money"));

        table1
            .window(Tumble.over(lit(1).hours()).on($("rowtime")).as("hourlyWindow"));

    }


    private static class TestPeriodicWatermarkerAssigner implements AssignerWithPeriodicWatermarks<Tuple3<Long, String, Integer>> {

        private final long maxOutOfOrderness = 0; // 10 seconds

        private long currentMaxTimestamp;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(Tuple3<Long, String, Integer> element, long recordTimestamp) {

            currentMaxTimestamp = Math.max(currentMaxTimestamp, element.f0);
            return element.f0;
        }
    }
}

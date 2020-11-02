package watermarks_lateness;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.scala.async.RichAsyncFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**

 1487225041000,001
 1487225049000,001
 1487225053000,001
 1487225046000,001
 1487225042000,001
 1487225057000,001
 1487225043000,001
 1487225058000,001
 1487225049000,001

 */
public class MyWatermarks {

    public static void main(String[] args) throws Exception {

//        The interval (every n milliseconds) in which the watermark will be generated is defined via ExecutionConfig.setAutoWatermarkInterval(...).

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> instream = env.socketTextStream("localhost", 9000, "\n");

        // 收集超过watermark和lateness的数据
        final OutputTag<String> lateOutputTag = new OutputTag<String>("late-data"){};

        SingleOutputStreamOperator<Tuple4<String, Long, Long, Long>> process =
                instream.assignTimestampsAndWatermarks(new TestPeriodicWatermarkerAssigner())
                .keyBy(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String s) throws Exception {
                        return s.split(",")[1];
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.seconds(5))
                .sideOutputLateData(lateOutputTag)
                .process(new ProcessWindowFunction<String, Tuple4<String, Long, Long, Long>, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<String> windowData,
                                        Collector<Tuple4<String, Long, Long, Long>> out) throws Exception {
                        Iterator<String> iterator = windowData.iterator();
                        while (iterator.hasNext()) {
                            long winStart = context.window().getStart();
                            long winEnd = context.window().getEnd();
                            String next = iterator.next();
                            System.out.println("window watermark:" + context.currentWatermark());
                            out.collect(new Tuple4<>(next.split(",")[1],
                                    Long.parseLong(next.split(",")[0]), winStart, winEnd));
                        }
                    }
                });


//        instream.keyBy(0).window(GlobalWindows.create())
//                .trigger(new Trigger<String, GlobalWindow>() {
//                    @Override
//                    public TriggerResult onElement(String element, long timestamp, GlobalWindow window, TriggerContext ctx) throws Exception {
//                        if (window.maxTimestamp() <= ctx.getCurrentWatermark()) {
//                            // if the watermark is already past the window fire immediately
//                            return TriggerResult.FIRE;
//                        } else {
//                            ctx.registerEventTimeTimer(window.maxTimestamp());
//                            return TriggerResult.CONTINUE;
//                        }
//                    }
//
//                    @Override
//                    public TriggerResult onProcessingTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
//                        return TriggerResult.CONTINUE;
//                    }
//
//                    @Override
//                    public TriggerResult onEventTime(long time, GlobalWindow window, TriggerContext ctx) throws Exception {
//                        return time == window.maxTimestamp() ?
//                                TriggerResult.FIRE :
//                                TriggerResult.CONTINUE;
//                    }
//
//                    @Override
//                    public void clear(GlobalWindow window, TriggerContext ctx) throws Exception {
//                        ctx.deleteEventTimeTimer(window.maxTimestamp());
//                    }
//                });
//        instream.keyBy(0).timeWindow(Time.seconds(5));
//        instream.keyBy(0).timeWindow(Time.seconds(10), Time.seconds(5));
//        instream.keyBy(0).countWindow(1000);
//        instream.keyBy(0).countWindow(1000, 100);

        DataStream<String> lateDatas = process.getSideOutput(lateOutputTag);

        DataStream<String> map1 = process.map(new MapFunction<Tuple4<String,Long, Long, Long>, String>() {

            @Override
            public String map(Tuple4<String, Long, Long, Long> data) throws Exception {
                return "winStart: " + data.f2 + ", winEnd: " + data.f3 + ", current data time : " + data.f1 + ", data content : " + data.f0;
            }
        });

        DataStream<String> map2 = lateDatas.map(new MapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {
                return "late----" + s;
            }
        });

        map1.print();
        map2.print();

        env.execute("test allowed lateness");

    }

    private static class TestPeriodicWatermarkerAssigner implements AssignerWithPeriodicWatermarks<String> {

        private final long maxOutOfOrderness = 3000; // 10 seconds

        private long currentMaxTimestamp;

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
        }

        @Override
        public long extractTimestamp(String data, long prevoisEventTimestamp) {

            String eventTime = data.split(",")[0];
            long timestamp = Long.parseLong(eventTime);

            currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
            System.out.println("data event time: " + eventTime + ", watermark:" + getCurrentWatermark());
            return timestamp;
        }
    }
}

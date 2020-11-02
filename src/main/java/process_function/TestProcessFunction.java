package yux.process_function;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.util.Collector;

public class TestProcessFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<OptLogs> stream = env.addSource(new SimpleSourceFunction())
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<OptLogs>() {
                    @Override
                    public long extractAscendingTimestamp(OptLogs element) {
                        return element.opts;
                    }
                });
        stream.print();

        DataStream<Tuple2<String, Long>> result =
            stream
            .keyBy(new KeySelector<OptLogs, String>() {
                @Override
                public String getKey(OptLogs value) throws Exception {
                    return value.userName;
                }
            }).process(new KeyedProcessFunction<String, OptLogs, Tuple2<String, Long>>() {

                /** 管理当前key（用户）的状态 */
                private ValueState<CountWithTimestamp> state;

                /** 从open中获取state */
                @Override
                public void open(Configuration parameters) throws Exception {
                    state = getRuntimeContext().getState(new ValueStateDescriptor<>("my_state", CountWithTimestamp.class));
                }

                @Override
                public void processElement(OptLogs value, Context ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                    CountWithTimestamp current = state.value();
                    if (current == null) {
                        current = new CountWithTimestamp();
                        current.key = value.userName;
                    }
                    current.count += 1;
                    current.lastModified = System.currentTimeMillis();
                    state.update(current);
                    ctx.timerService().registerEventTimeTimer(current.lastModified + 9000);
                }

                @Override
                public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Long>> out) throws Exception {
                    CountWithTimestamp current = state.value();
                    if ((current.lastModified + 9000) == timestamp) {
                        out.collect(new Tuple2<String, Long>(current.key, current.count));
                    }
                }
            });

        result.print();

        env.execute("run process function");


    }

    @Getter
    @Setter
    @ToString
    public static class OptLogs {
        // 用户名
        private String userName;
        // 操作类型
        private int optType;
        // 时间戳
        private long opts;

        public OptLogs(String userName, int optType, long opts) {
            this.userName = userName;
            this.optType = optType;
            this.opts = opts;
        }
    }

    /**
     * state中存的数据
     * key：OptLogs.userName
     * count: 操作次数
     * lastModified：最后一次操作时间
     */
    public static class CountWithTimestamp {
        public String key;
        public long count;
        public long lastModified;
    }

    public static final String[] nameArray = new String[] {
            "张三",
            "李四",
            "王五",
            "赵六",
            "钱七"
    };

    private static class SimpleSourceFunction implements SourceFunction<OptLogs>{

        private long num = 0L;
        private volatile boolean isRunning = true;

        @Override
        public void run(SourceContext<OptLogs> ctx) throws Exception {
            while (isRunning){
                int randomNum = (int)(Math.random() * 5);
                OptLogs optLog = new OptLogs(nameArray[randomNum], randomNum, System.currentTimeMillis());
                ctx.collect(optLog);
                Thread.sleep(3000);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }
}

package yux.broadcast.UserPurchaseBehavior;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pulsar.PulsarSourceBuilder;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import yux.broadcast.UserPurchaseBehavior.common.MyPeriodWaterMark;
import yux.broadcast.UserPurchaseBehavior.common.Params;
import yux.broadcast.UserPurchaseBehavior.common.Utils;

import java.util.concurrent.TimeUnit;

public class Launcher {

    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        ParameterTool params = Utils.parameterCheck(args);

        // 让页面可以打印传入的参数信息
        env.getConfig().setGlobalJobParameters(params);

        // 设置时间为event time
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 设置checkpoint
        env.enableCheckpointing(60000L);
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        checkpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        checkpointConfig.setMinPauseBetweenCheckpoints(30000L);
        checkpointConfig.setCheckpointTimeout(10000L);
        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // stateBackend
        env.setStateBackend(new FsStateBackend("hdfs://tmp/flink_ck/"));

        // restart 策略
        env.setRestartStrategy(
                RestartStrategies
                        .fixedDelayRestart(10, Time.of(30, TimeUnit.SECONDS))
        );

        // 事件流 数据
        PulsarSourceBuilder<Row> pulsarSourceBuilder = PulsarSourceBuilder
                .builder(new AvroRowDeserializationSchema(Params.USER_EVENT_SCHEMA))
                .serviceUrl(params.get(Params.SERVER_URL));

        ConsumerConfigurationData<byte[]> consumerConf = new ConsumerConfigurationData();
        consumerConf.setTopicNames(Utils.getTopics(params.get(Params.TOPICS)));
        consumerConf.setSubscriptionInitialPosition(SubscriptionInitialPosition.Latest);
        consumerConf.setSubscriptionType(SubscriptionType.Shared);
        consumerConf.setSubscriptionName("userPurchase");
        pulsarSourceBuilder.pulsarAllConsumerConf(consumerConf);

        SourceFunction<Row> pulsarSrc = null;
        try {
            pulsarSrc = pulsarSourceBuilder.build();
        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
        // pulsar source
        DataStreamSource<Row> pulsarSource = env.addSource(pulsarSrc);

        KeyedStream<Row, String> eventSource = pulsarSource
                .assignTimestampsAndWatermarks(new MyPeriodWaterMark())
                .keyBy(new KeySelector<Row, String>() {
                    @Override
                    public String getKey(Row value) throws Exception {
                        return (String) value.getField(1);
                    }
                });

        // 配置流 数据







    }
}

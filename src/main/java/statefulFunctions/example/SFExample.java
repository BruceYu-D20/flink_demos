package statefulFunctions.example;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.statefun.flink.core.StatefulFunctionsConfig;
import org.apache.flink.statefun.flink.core.message.MessageFactoryType;
import org.apache.flink.statefun.flink.core.message.RoutableMessage;
import org.apache.flink.statefun.flink.core.message.RoutableMessageBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionDataStreamBuilder;
import org.apache.flink.statefun.flink.datastream.StatefulFunctionEgressStreams;
import org.apache.flink.statefun.sdk.Context;
import org.apache.flink.statefun.sdk.FunctionType;
import org.apache.flink.statefun.sdk.StatefulFunction;
import org.apache.flink.statefun.sdk.annotations.Persisted;
import org.apache.flink.statefun.sdk.io.EgressIdentifier;
import org.apache.flink.statefun.sdk.state.PersistedTable;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.net.URI;
import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.statefun.flink.datastream.RequestReplyFunctionBuilder.requestReplyFunctionBuilder;

public class SFExample {

    private static FunctionType GREET = new FunctionType("example", "greet");
    private static final FunctionType REMOTE_GREET = new FunctionType("example", "remote-greet");
    private static final EgressIdentifier<String> GREETINGS =
            new EgressIdentifier<>("example", "out", String.class);

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StatefulFunctionsConfig statefunConfig = StatefulFunctionsConfig.fromEnvironment(env);
        statefunConfig.setFactoryType(MessageFactoryType.WITH_KRYO_PAYLOADS);

        DataStream<RoutableMessage> names = env.addSource(new NameSource())
            .map(new MapFunction<String, RoutableMessage>() {
                @Override
                public RoutableMessage map(String message) throws Exception {

                    String keyAsId = message.split(",")[0];

                    RoutableMessage routableMessage = RoutableMessageBuilder.builder()
                            // FunctionType是定的，但id是自己定义的，将什么消息发送到什么id从这里区分，类似于电脑的端口
                            .withTargetAddress(GREET, keyAsId)
                            // ingress的写什么内容
                            .withMessageBody(message)
                            .build();
                    return routableMessage;
                }
            });

        SingleOutputStreamOperator<RoutableMessage> names2 = env.addSource(new NameSourceCity())
                .map(new MapFunction<String, RoutableMessage>() {
                    @Override
                    public RoutableMessage map(String message) throws Exception {
                        String keyAsId = message.split(",")[0];
                        RoutableMessage routableMessage = RoutableMessageBuilder.builder()
                                // FunctionType是定的，但id是自己定义的，将什么消息发送到什么id从这里区分，类似于电脑的端口
                                .withTargetAddress(GREET, keyAsId)
                                // ingress的写什么内容
                                .withMessageBody(message)
                                .build();
                        return routableMessage;
                    }
                });

        StatefulFunctionEgressStreams egresses = StatefulFunctionDataStreamBuilder.builder("example")
                .withDataStreamAsIngress(names)
                .withDataStreamAsIngress(names2)
                // 将stateful function绑定到FunctionType上
                .withFunctionProvider(GREET, unused -> new joinCCAndListFunction())
                .withRequestReplyRemoteFunction(
                        requestReplyFunctionBuilder(
                                REMOTE_GREET, URI.create("http://localhost:5000/statefun"))
                                .withPersistedState("seen_count")
                                .withMaxRequestDuration(Duration.ofSeconds(15))
                                .withMaxNumBatchRequests(500))
                .withEgressId(GREETINGS)
                .withConfiguration(statefunConfig)
                .build(env);

        DataStream<String> greetingsEgress = egresses.getDataStreamForEgressId(GREETINGS);

        greetingsEgress
                .map(new RichMapFunction<String, String>() {
                    @Override
                    public String map(String value) throws Exception {
                        return "value: " + value;
                    }
                }).addSink(new PrintSinkFunction<>());

        env.execute();


    }

    private static final class joinCCAndListFunction implements StatefulFunction {

        @Persisted
        private final PersistedTable<String, String> ccAndList = PersistedTable.of("ccandlist", String.class, String.class);

        @Override
        public void invoke(Context context, Object input) {
            Tuple2<String, String> keyAndValue = getKeyAndValue(input.toString());
            context.send(GREETINGS, String.format("key:%s    message:%s", keyAndValue.f0, keyAndValue.f1));
        }

        public Tuple2<String, String> getKeyAndValue(String input) {
            String[] message = input.split(",");
            String key = message[0];
            String value = message[1];
            String currentMessage = "";
            if(ccAndList.get(key) == null){
                currentMessage = value;
                ccAndList.set(key, key);
            }else {
                currentMessage = ccAndList.get(key) + "_" + value;
                ccAndList.remove(key);
            }
            return Tuple2.of(key, currentMessage);
        }
    }

    private static final class NameSource implements SourceFunction<String> {

        private static final long serialVersionUID = 1;

        private volatile boolean canceled;

        @Override
        public void run(SourceFunction.SourceContext<String> ctx) throws InterruptedException {
            String[] names = {"key1,cc1", "key2,cc2", "key3,cc3"};
            ThreadLocalRandom random = ThreadLocalRandom.current();
            while (true) {
                int index = random.nextInt(names.length);
                final String name = names[index];
                synchronized (ctx.getCheckpointLock()) {
                    if (canceled) {
                        return;
                    }
                    ctx.collect(name);
                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            canceled = true;
        }
    }

    private static final class NameSourceCity implements SourceFunction<String> {

        private static final long serialVersionUID = 1;

        private volatile boolean canceled;

        @Override
        public void run(SourceFunction.SourceContext<String> ctx) throws InterruptedException {
            String[] names = {"key1,cclist1", "key2,cclist2"};
            ThreadLocalRandom random = ThreadLocalRandom.current();
            while (true) {
                int index = random.nextInt(names.length);
                final String name = names[index];
                synchronized (ctx.getCheckpointLock()) {
                    if (canceled) {
                        return;
                    }
                    ctx.collect(name);
                }
                Thread.sleep(1000);
            }
        }

        @Override
        public void cancel() {
            canceled = true;
        }
    }
}

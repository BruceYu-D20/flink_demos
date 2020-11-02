package flink_udf;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.formats.avro.AvroRowDeserializationSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.pulsar.PulsarSourceBuilder;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.types.Row;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;
import org.junit.Test;
import yux.pulsar.Utils;

import java.util.HashSet;
import java.util.Set;

public class TestUDF extends AbstractTestBase {

    @Test
    public void myudf() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        PulsarSourceBuilder<Row> build =
                PulsarSourceBuilder
                        .builder(new AvroRowDeserializationSchema(Utils.PULSAR_HDFS_SCHEMA))
                        .acknowledgementBatchSize(10)
                        .acknowledgementBatchSize(100)
                        .serviceUrl("pulsar://172.16.108.110:6650");

        Set<String> topics = new HashSet<>();
        topics.add("persistent://public/default/pulsar-hdfs-sink-topic");

        ConsumerConfigurationData<byte[]> consumerConfigurationData = new ConsumerConfigurationData<>();
        consumerConfigurationData.setSubscriptionType(SubscriptionType.Shared);
        consumerConfigurationData.setSubscriptionInitialPosition(SubscriptionInitialPosition.Latest);
        consumerConfigurationData.setTopicNames(topics);
        consumerConfigurationData.setSubscriptionName("my_subscrib2");

        build.pulsarAllConsumerConf(consumerConfigurationData);

        SourceFunction<Row> pulsarStream = build.build();

        DataStreamSource<Row> pulsarSource = env.addSource(pulsarStream);

        tableEnv.registerDataStream("pulsar_source_table", pulsarSource, "fname,id");

        tableEnv.registerFunction("str_concat", new StringConcatUDF());

        String sql = "SELECT id,fname,str_concat(fname,fname) FROM pulsar_source_table";

        Table table = tableEnv.sqlQuery(sql);

        tableEnv.toAppendStream(table,Row.class).print();

        env.execute("test_table");
    }

    @Test
    public void myudtf() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.enableCheckpointing(5000);

        PulsarSourceBuilder<Row> build =
                PulsarSourceBuilder
                        .builder(new AvroRowDeserializationSchema(Utils.PULSAR_HDFS_SCHEMA))
                        .acknowledgementBatchSize(10)
                        .acknowledgementBatchSize(100)
                        .serviceUrl("pulsar://172.16.108.110:6650");

        Set<String> topics = new HashSet<>();
        topics.add("persistent://public/default/pulsar-hdfs-sink-topic");

        ConsumerConfigurationData<byte[]> consumerConfigurationData = new ConsumerConfigurationData<>();
        consumerConfigurationData.setSubscriptionType(SubscriptionType.Shared);
        consumerConfigurationData.setSubscriptionInitialPosition(SubscriptionInitialPosition.Latest);
        consumerConfigurationData.setTopicNames(topics);
        consumerConfigurationData.setSubscriptionName("my_subscrib2");

        build.pulsarAllConsumerConf(consumerConfigurationData);

        SourceFunction<Row> pulsarStream = build.build();

        DataStreamSource<Row> pulsarSource = env.addSource(pulsarStream);

        tableEnv.registerDataStream("my_udf_table", pulsarSource, "fname,id");
        tableEnv.registerFunction("my_udtf", new String_Split_UDTF());

        String udtfSql = "SELECT id,fname,data1,data2 FROM my_udf_table," +
                "lateral table(my_udtf(fname)) " +
                "as T(data1,data2)";

        Table udtfTable = tableEnv.sqlQuery(udtfSql);
        tableEnv.toAppendStream(udtfTable, Row.class).print();

        env.execute("test_table");
    }

    @Test
    public void myUdaf() throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);

        PulsarSourceBuilder<Row> builder = PulsarSourceBuilder.builder(new AvroRowDeserializationSchema(Utils.PULSAR_HDFS_SCHEMA))
                .serviceUrl("pulsar://172.16.108.110:6650");

        Set<String> topics = new HashSet<>();
        topics.add("persistent://public/default/pulsar-hdfs-sink-topic");

        ConsumerConfigurationData<byte[]> consumerConfigurationData = new ConsumerConfigurationData<>();
        consumerConfigurationData.setSubscriptionType(SubscriptionType.Exclusive);
        consumerConfigurationData.setSubscriptionInitialPosition(SubscriptionInitialPosition.Latest);
        consumerConfigurationData.setTopicNames(topics);
        consumerConfigurationData.setSubscriptionName("my_subscrib4");

        builder.pulsarAllConsumerConf(consumerConfigurationData);

        SourceFunction<Row> pulsarStream = builder.build();

        DataStreamSource<Row> pulsarSource = env.addSource(pulsarStream);

        tableEnv.registerDataStream("pulsar_source_table", pulsarSource, "fname,id,rowtime.rowtime");
//        tableEnv.registerDataStream("pulsar_source_table", pulsarSource, "fname,id");
        tableEnv.registerFunction("count_udaf", new Count_UDAF());

        String sql = "SELECT " +
                "TUMBLE_START(rowtime, INTERVAL '10' SECOND) as window_start," +
                "TUMBLE_END(rowtime, INTERVAL '10' SECOND) as window_start," +
                "id,fname,count_udaf(1) " +
                "FROM pulsar_source_table " +
                "GROUP BY id,fname,TUMBLE(rowtime, INTERVAL '10' SECOND)";

        Table table = tableEnv.sqlQuery(sql);
        tableEnv.toAppendStream(table, Row.class).print();

        env.execute("udaf test");

    }

    @Test
    public void myUdtf2Udaf() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.enableCheckpointing(5000);

        PulsarSourceBuilder<Row> builder = PulsarSourceBuilder.builder(new AvroRowDeserializationSchema(Utils.PULSAR_HDFS_SCHEMA))
                .acknowledgementBatchSize(10)
                .acknowledgementBatchSize(100)
                .serviceUrl("pulsar://172.16.108.110:6650");

        Set<String> topics = new HashSet<>();
        topics.add("persistent://public/default/pulsar-hdfs-sink-topic");

        ConsumerConfigurationData<byte[]> consumerConfigurationData = new ConsumerConfigurationData<>();
        consumerConfigurationData.setSubscriptionType(SubscriptionType.Shared);
        consumerConfigurationData.setSubscriptionInitialPosition(SubscriptionInitialPosition.Latest);
        consumerConfigurationData.setTopicNames(topics);
        consumerConfigurationData.setSubscriptionName("my_subscrib3");

        builder.pulsarAllConsumerConf(consumerConfigurationData);

        SourceFunction<Row> pulsarStream = builder.build();

        DataStreamSource<Row> pulsarSource = env.addSource(pulsarStream);

        tableEnv.registerDataStream("pulsar_source_table", pulsarSource, "fname,id");
        tableEnv.registerFunction("str_concat_udf", new StringConcatUDF());

        String sql = "SELECT id,fname,str_concat_udf(fname,fname) as fname_conc FROM pulsar_source_table";

        Table table = tableEnv.sqlQuery(sql);

        DataStream<Row> udfDF = tableEnv.toAppendStream(table, Row.class)
                .map(new MapFunction<Row, Row>() {
                    @Override
                    public Row map(Row value) throws Exception {
                        return value;
                    }
                }).returns(new RowTypeInfo(Types.INT, Types.STRING, Types.STRING));

        tableEnv.registerDataStream("udfDF_table", udfDF, "id,fname,fname_conc");
        tableEnv.registerFunction("str_split_udaf", new String_Split_UDTF());

        String udafSql = "SELECT id,fname,fname_conc,data1,data2 " +
                "FROM udfDF_table," +
                "lateral table(str_split_udaf(fname_conc)) " +
                "as T(data1,data2)";
        Table udaf_table = tableEnv.sqlQuery(udafSql);
        tableEnv.toAppendStream(udaf_table, Row.class).print();


        env.execute("udf 2 udtf");
    }
}

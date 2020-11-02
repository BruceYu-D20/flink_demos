package redis_asyn;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.Collector;

import java.util.List;

public class Msisdn5Key {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TableEnvironment tenv = TableEnvironment.create(null);
        EnvironmentSettings.newInstance();

        // 模拟cc数据
        DataStream<CC> ccSource = env.socketTextStream("localhost", 9988)
                .map(new MapFunction<String, CC>() {
                    @Override
                    public CC map(String value) throws Exception {
                        String msisdn = value.split(",")[0];
                        String cctext = value.split(",")[1];
                        CC cc = new CC();
                        cc.setMsisdn(msisdn);
                        cc.setText(cctext);
                        return cc;
                    }
                });
/**
 *

        // 模拟cclist数据
        DataStream<CCList> cclistSource = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, CCList>() {
                    @Override
                    public CCList map(String value) throws Exception {
                        String msisdn = value.split(",")[0];
                        String listText = value.split(",")[1];
                        CCList cclist = new CCList();
                        cclist.setMsisdn(msisdn);
                        cclist.setListText(listText);
                        return cclist;
                    }
                });
 */
        // 模拟下发的msisdn数据
        DataStreamSource<List<String>> mysqlSource = env.addSource(new MysqlMsisdnsSource());

        MapStateDescriptor<Void, List<String>> msisdnsMapDesc =
                new MapStateDescriptor<>("msisdns", Types.VOID, Types.LIST(Types.STRING));
        BroadcastStream<List<String>> msisdnBroadcast = mysqlSource.broadcast(msisdnsMapDesc);

        BroadcastConnectedStream<CC, List<String>> ccAndMsisdnStream = ccSource.connect(msisdnBroadcast);
        DataStream<CC> ccProcessedStream = ccAndMsisdnStream.process(new ccMsisdnBroadcastProcessFunction(msisdnsMapDesc));

        ccProcessedStream.print();

        env.execute("run job");

//        BroadcastConnectedStream<CCList, List<String>> cclistMsisdnStream = cclistSource.connect(msisdnBroadcast);


    }

    private static class ccMsisdnBroadcastProcessFunction extends BroadcastProcessFunction<CC, List<String>, CC> {

        private MapStateDescriptor<Void, List<String>> msisdnsMapDesc;

        public ccMsisdnBroadcastProcessFunction(MapStateDescriptor<Void, List<String>> msisdnsMapDesc){
            this.msisdnsMapDesc = msisdnsMapDesc;
        }

        @Override
        public void processElement(CC cc, ReadOnlyContext ctx, Collector<CC> out) throws Exception {

            ReadOnlyBroadcastState<Void, List<String>> msisdnBroadcast = ctx.getBroadcastState(msisdnsMapDesc);
            List<String> msisdns = msisdnBroadcast.get(null);

            if(msisdns.contains(cc.getMsisdn())){
                out.collect(cc);
            }

        }

        @Override
        public void processBroadcastElement(List<String> value, Context ctx, Collector<CC> out) throws Exception {

            BroadcastState<Void, List<String>> msisdnBroadcast = ctx.getBroadcastState(msisdnsMapDesc);
            msisdnBroadcast.clear();
            msisdnBroadcast.put(null, value);
        }
    }

}

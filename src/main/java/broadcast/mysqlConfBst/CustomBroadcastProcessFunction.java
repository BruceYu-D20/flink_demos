package broadcast.mysqlConfBst;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.util.HashMap;
import java.util.Map;

/**
 *
 * 自定义BroadcastProcessFunction
 *
 * 当事件流中的用户ID在配置中出现时，才对该事件处理, 并在事件中补全用户的基础信息
 *
 * Tuple2<Long, String>是事件流。在本例子中是nc -l 9000的数据
 * HashMap<String, Tuple2<String, Integer>>是数据流。在本例子中是mysql中的数据
 * Tuple4<Long, String, String, Integer>是返回结果的数据流的类型
 * Tuple4是 custom.timestamp, custom.id, mysql.name, mysql.age
 *
 */

public class CustomBroadcastProcessFunction extends
        BroadcastProcessFunction<Tuple2<Long, String>,
                HashMap<String, Tuple2<String, Integer>>,
                Tuple4<Long, String, String, Integer>> {

    /**定义MapStateDescriptor*/
//    MapStateDescriptor<Void, Map<String, Tuple2<String,Integer>>> configDescriptor =
//            new MapStateDescriptor<>("config", Types.VOID,
//                    Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));

    private final MapStateDescriptor configDescriptor;

    public CustomBroadcastProcessFunction(MapStateDescriptor configDescriptor){
        this.configDescriptor = configDescriptor;
    }

    /**
     * 读取状态，并基于状态，处理事件流中的数据
     * 在这里，从上下文中获取状态，基于获取的状态，对事件流中的数据进行处理
     * @param value 事件流中的数据
     * @param ctx 上下文
     * @param out 输出零条或多条数据
     * @throws Exception
     */
    @Override
    public void processElement(Tuple2<Long, String> value, ReadOnlyContext ctx,
                               Collector<Tuple4<Long, String, String, Integer>> out) throws Exception {

        //nc -l 中的用户ID
        String userID = value.f1;
        System.out.println("userid: " + userID);

        //获取状态-mysql
        ReadOnlyBroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState =
                ctx.getBroadcastState(configDescriptor);
        Map<String, Tuple2<String, Integer>> mysqlDataInfo = broadcastState.get(null);
        System.out.println("广播内容： " + mysqlDataInfo);

        //配置中有此用户，则在该事件中添加用户的userName、userAge字段。
        //配置中没有此用户，则丢弃
        Tuple2<String, Integer> userInfo = mysqlDataInfo.get(userID);
        System.out.println(mysqlDataInfo);
        if(userInfo!=null){
            out.collect(new Tuple4<>(value.f0, value.f1, userInfo.f0, userInfo.f1));
        }else{
            System.out.println("没有该用户 " + userID);
        }

    }

    /**
     * 处理广播流中的每一条数据，并更新状态
     * @param value 广播流中的数据
     * @param ctx 上下文
     * @param out 输出零条或多条数据
     * @throws Exception
     */
    @Override
    public void processBroadcastElement(HashMap<String, Tuple2<String, Integer>> value, Context ctx, Collector<Tuple4<Long, String, String, Integer>> out) throws Exception {

        // 获取状态
        BroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(configDescriptor);

        //更新状态
        broadcastState.put(null,value);
    }
}

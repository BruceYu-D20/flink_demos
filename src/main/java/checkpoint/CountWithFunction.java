package yux.checkpoint;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.util.Collector;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * 想知道两次事件1之间，一共发生多少次其他事件，分别是什么事件
 *
 * 事件流：1 2 3 4 5 1 3 4 5 6 7 1 4 5 3 9 9 2 1...
 * 输出：
 *      (4,2 3 4 5)
 *      (5,3 4 5 6 7)
 *      (6,4 5 6 9 9 2)
 */
public class CountWithFunction extends RichFlatMapFunction<Integer, Tuple2<Integer, String>> implements CheckpointedFunction {

    /**
     * 托管状态
     */
    private transient ListState<Integer> checkpointCountList;

    /**
     * 原始状态
     */
    private List<Integer> rawState;

    @Override
    public void flatMap(Integer value, Collector<Tuple2<Integer, String>> out) throws Exception {

        if(value == 1){
            if(rawState.size() > 0){
                StringBuilder builder = new StringBuilder();
                for(Integer element: rawState){
                    builder.append(element + " ");
                }
                out.collect(new Tuple2<>(rawState.size(), builder.toString()));
                rawState.clear();
            }
        }else {
            rawState.add(value);
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkpointCountList.clear();
        checkpointCountList.addAll(rawState);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor<Integer> checkpointCountListDesc =
                new ListStateDescriptor<>("checkpointCountListDesc", TypeInformation.of(new TypeHint<Integer>() {}));
        checkpointCountList = context.getOperatorStateStore().getListState(checkpointCountListDesc);
        if(context.isRestored()){
            for(Integer element: checkpointCountList.get()){
                rawState.add(element);
            }
        }

    }

    @Override
    public void open(Configuration parameters) throws Exception {
        rawState = new ArrayList<>();
    }
}

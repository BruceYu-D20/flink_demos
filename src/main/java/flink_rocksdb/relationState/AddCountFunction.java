package flink_rocksdb.relationState;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;

import java.util.List;

public class AddCountFunction extends RichMapFunction<String, Integer> implements CheckpointedFunction{

    private transient ListState<Integer> checkpointCountList;

    private Integer rawState;

    @Override
    public Integer map(String value) throws Exception {

        rawState = rawState + Integer.parseInt(value);
        return rawState;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {

        checkpointCountList.clear();
        checkpointCountList.add(rawState);

    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        ListStateDescriptor<Integer> desc =
                new ListStateDescriptor<Integer>("checkpoint", TypeInformation.of(new TypeHint<Integer>() {}));

        checkpointCountList = context.getOperatorStateStore().getListState(desc);

        if(context.isRestored()){
            for(Integer state: checkpointCountList.get()){
                rawState = state;
            }
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if(rawState == null){
            rawState = 0;
        }
    }
}

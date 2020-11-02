package yux.operator_state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class CountWithOperatorListState extends RichFlatMapFunction<Long, Tuple2<Integer, String>> implements ListCheckpointed<Long>{

    /**
     * raw state
     * @param value
     * @param out
     * @throws Exception
     */
    private List<Long> listBufferElements;


    @Override
    public void flatMap(Long value, Collector<Tuple2<Integer, String>> out) throws Exception {

        if(value == 1){
            if(listBufferElements.size() > 0){
                StringBuffer buffer = new StringBuffer();
                for(Long item: listBufferElements){
                    buffer.append(item + "  ");
                }
                out.collect(new Tuple2<Integer, String>(listBufferElements.size(), buffer.toString()));
                listBufferElements.clear();
            }
        }else{
            listBufferElements.add(value);
        }
    }

    @Override
    public List<Long> snapshotState(long checkpointId, long timestamp) throws Exception {
        return listBufferElements;
    }

    @Override
    public void restoreState(List<Long> state) throws Exception {
        for(Long item: state){
            listBufferElements.add(item);
        }
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        listBufferElements = new ArrayList<>();
    }
}

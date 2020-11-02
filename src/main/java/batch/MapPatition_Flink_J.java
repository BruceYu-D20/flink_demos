package batch;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.util.Collector;

public class MapPatition_Flink_J {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Long> dset = env.generateSequence(1, 1000);
        
        DataSet result = dset
            .filter(new FilterFunction<Long>() {
                @Override
                public boolean filter(Long a) throws Exception {
                    return a % 2 == 0;
                }
            })
            .mapPartition(new MapPartitionFunction<Long, Long>() {
                @Override
                public void mapPartition(Iterable<Long> values, Collector<Long> out) throws Exception {
                    Long count = 0L;
                    for(Long value: values){
                        count++;
                    }
                    out.collect(count);
                }
            });

        result.print();
    }
}

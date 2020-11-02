package yux.keyedstate;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class KeyedCountState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Long>> {

    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ValueStateDescriptor stateDescriptor =
                new ValueStateDescriptor("valueState", TypeInformation.of(new TypeHint<Tuple2<Long, Long>>() {}));
        sum = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Long>> out) throws Exception {

        Tuple2<Long, Long> currentSum = sum.value();

        if(currentSum == null){
            currentSum = Tuple2.of(0L, 0L);
        }

        currentSum.f0 += 1;
        currentSum.f1 += value.f1;
        sum.update(currentSum);

        if(currentSum.f0 >= 3){
            out.collect(Tuple2.of(value.f0, currentSum.f1 / currentSum.f0));
            sum.clear();
        }
    }
}

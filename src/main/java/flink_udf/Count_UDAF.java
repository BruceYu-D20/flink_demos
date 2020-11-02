package flink_udf;

import org.apache.flink.table.functions.AggregateFunction;

public class Count_UDAF extends AggregateFunction<Long, Count_UDAF.MySumAccumulator>{


    /**
     * 定义一个Accumulator，存放聚合的中间结果
     */
    public static class MySumAccumulator{
        public long sumPrice;
    }

    @Override
    public MySumAccumulator createAccumulator() {
        MySumAccumulator acc = new MySumAccumulator();
        acc.sumPrice = 0;
        return acc;
    }

    public void accumulate(MySumAccumulator acc, int price){
        acc.sumPrice += price;
    }

    @Override
    public Long getValue(MySumAccumulator acc) {
        return acc.sumPrice;
    }
}

package dataset_api;

import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import static sun.misc.Version.print;

public class DatasetTest {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<Tuple4> source = env.fromElements(
                new Tuple4(1, "zhangsan", "male", 19),
                new Tuple4(2, "lisi", "male", 20),
                new Tuple4(3, "wangwu", "female", 19),
                new Tuple4(4, "zhaoliu", "male", 19),
                new Tuple4(5, "ln", "female", 20),
                new Tuple4(6, "jack", "male", 21),
                new Tuple4(7, "rose", "male", 22));

        // 位置索引
        // source.project(1).print();

        // groupBy
        // 四种方式:
        // key expression
        // key-selector
        // field position
        // scala case class
        // dataset的分组内排序
        source.groupBy(2).sortGroup(3, Order.DESCENDING).first(2).print();
        // groupby,reduce,reduceGroup(一次性获取所有数据，输出任意个元素),combineGroup
        // aggregate(sum/min/minby/max/maxby)

        System.out.println("------");

        // dataset的partition内排序
        source.setParallelism(2).sortPartition(0, Order.ASCENDING).sortPartition(3, Order.DESCENDING)
            .mapPartition(new RichMapPartitionFunction<Tuple4, Tuple4>() {
                @Override
                public void mapPartition(Iterable<Tuple4> values, Collector<Tuple4> out) throws Exception {
                    for(Tuple4 item: values){
                        System.out.println(getRuntimeContext().getIndexOfThisSubtask() + "," + item);
                    }
                }
            }).returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING, Types.INT)).print();

        //distinct 去重

        // joinFunction 和 flatjoinfunction
        // joinWithHuge

    }
}

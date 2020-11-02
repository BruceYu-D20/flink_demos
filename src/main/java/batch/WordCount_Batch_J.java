package batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount_Batch_J {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<String> text =
                env.fromElements("this is my word", "first word count");

        DataSet<Tuple2<String, Integer>> counts =
                text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String values, Collector<Tuple2<String, Integer>> out) throws Exception {
                        for (String value : values.split("\\s")) {
                            out.collect(new Tuple2<>(value, 1));
                        }
                    }
                })
                .groupBy(0)
                .sum(1);

        counts.print();
    }
}

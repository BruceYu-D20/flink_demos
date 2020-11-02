package yux.dataset_api;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.IterativeDataSet;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.utils.DataSetUtils;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeUnit;


public class DSIteratorTest extends AbstractTestBase {

    ExecutionEnvironment env;

    @Before
    public void createEnv(){
        env = ExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                    3,
                    Time.of(10, TimeUnit.SECONDS)));
    }

    @Test
    public void testitrator() throws Exception {

        int maxIteratorCnt = 10;
        DataSource<Long> input = env.generateSequence(1, 10);

        IterativeDataSet<Long> initial = input.iterate(maxIteratorCnt);

        DataSet<Long> iteration = initial.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long in) throws Exception {
                return in + 1L;
            }
        });

        DataSet<Long> resultData = initial.closeWith(iteration);

        resultData.print();
    }

    @Test
    public void testPi() throws Exception {
        int maxIteratorCnt = 100;

        IterativeDataSet<Integer> initial = env.fromElements(0).iterate(maxIteratorCnt);

        DataSet<Integer> iterator = initial.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {

                double x = Math.random();
                double y = Math.random();
                return value + ((x * x + y * y <= 1) ? 1 : 0);
            }
        });

        DataSet<Integer> count = initial.closeWith(iterator);

        count.map(new MapFunction<Integer, Double>() {
            @Override
            public Double map(Integer value) throws Exception {
                return value / (double)maxIteratorCnt * 4;
            }
        }).print();

    }

    @Test
    public void testDataSetUtils() throws Exception {

        DataSet<String> input = env.fromElements("a", "b", "c", "d", "e", "f").setParallelism(3);
        DataSetUtils.countElementsPerPartition(input).print();

        System.out.println("***************");
        DataSetUtils.zipWithIndex(input).print();

        System.out.println("***************");
        DataSetUtils.zipWithUniqueId(input).print();
    }
}

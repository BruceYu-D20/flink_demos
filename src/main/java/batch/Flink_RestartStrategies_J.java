package batch;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.runtime.executiongraph.restart.RestartStrategy;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class Flink_RestartStrategies_J {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 固定延时的任务重启
        /**
        env.setRestartStrategy(
                RestartStrategies.
                        fixedDelayRestart(2, Time.of(10, TimeUnit.SECONDS)));
         */

        // 失败率重试任务：1h内允许重启两次，每次失败固定等待时间10s
        env.setRestartStrategy(
                RestartStrategies.failureRateRestart(
                        2,
                        Time.of(1, TimeUnit.HOURS),
                        Time.of(10, TimeUnit.SECONDS)
                )
        );

        DataSet<String> ds1 = env.fromElements("1", "2", "", "4");

        DataSet ds2 = ds1.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String s) throws Exception {
                return Integer.parseInt(s);
            }
        });

        ds2.print();
    }
}

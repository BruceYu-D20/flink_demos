package batch;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.configuration.Configuration;

/**
 * 通过构造器传递参数或通过withParameters传递参数
 */
public class Flink_ParamPass_J {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /**
         * 1. 通过withParameters
         */

        Configuration conf = new Configuration();
        conf.setInteger("mylimit", 1);

        DataSet<Integer> ds1 = env.fromElements(-1, 1, 2, 3, -2);

        DataSet<Integer> ds2 = ds1.filter(new RichFilterFunction<Integer>() {

            private int mylimit;

            @Override
            public void open(Configuration parameters) throws Exception {
                mylimit = parameters.getInteger("mylimit", 1);
            }

            @Override
            public boolean filter(Integer value) throws Exception {
                return value > mylimit;
            }
        }).withParameters(conf);

//        ds2.print();

        /**
         * 通过全局参数
         */
        env.getConfig().setGlobalJobParameters(conf);

        DataSet<Integer> ds3 = ds1.filter(new RichFilterFunction<Integer>() {

            private int mylimit;

            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("--");
                Configuration globalConf = (Configuration)getRuntimeContext()
                        .getExecutionConfig().getGlobalJobParameters();

                mylimit = globalConf.getInteger("mylimit", 1);

            }

            @Override
            public boolean filter(Integer value) throws Exception {
                System.out.println("**");
                return value > mylimit;
            }
        });
        ds3.print();



    }
}

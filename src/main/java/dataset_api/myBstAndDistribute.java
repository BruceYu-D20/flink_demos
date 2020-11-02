package yux.dataset_api;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.junit.Test;
import org.junit.Before;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class myBstAndDistribute extends AbstractTestBase {

    ExecutionEnvironment env;

    @Before
    public void createEnv(){
        env = ExecutionEnvironment.getExecutionEnvironment();
    }

    /**
     * 广播给每个taskmanager分发一份
     *
     * @throws Exception
     */
    @Test
    public void testBroadcast() throws Exception {

        DataSet<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7);

        // 要被广播的数据集
        DataSet<Integer> bstSource = env.fromElements(2,4,6);

        source.flatMap(new RichFlatMapFunction<Integer, Integer>() {

            Set<Integer> bstData;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                List<Integer> bstDataSet = getRuntimeContext().getBroadcastVariable("bstDataSet");
                bstData = new HashSet<Integer>(bstDataSet);
            }

            @Override
            public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                if(bstData.contains(value)){
                    out.collect(value);
                }
            }
        }).withBroadcastSet(bstSource, "bstDataSet").print();
    }

    /**
     *
     * Distributed Cache File 会把远程文件系统的文件放到每个taskmanager所在机器的文件系统中
     * 相当于从本地文件系统取文件内容
     *
     * @throws Exception
     */
    @Test
    public void testDistributeFile() throws Exception {

        // 2,4,6
        env.registerCachedFile("file:///Users/yuxiang/codes/flink_test/src/main/java/yux/dataset_api/distributefile.txt", "myfile");

        DataSet<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7);

        source.flatMap(new RichFlatMapFunction<Integer, Integer>() {

            Set<Integer> distributeFile;
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                distributeFile = new HashSet<>();

                File file = getRuntimeContext().getDistributedCache().getFile("myfile");
                BufferedReader buffer = new BufferedReader(new FileReader(file));
                String line;
                while(null != (line = buffer.readLine())){
                    String[] split = line.split(",");
                    for(String num: split){
                        distributeFile.add(new Integer(num));
                    }
                }
            }

            @Override
            public void flatMap(Integer value, Collector<Integer> out) throws Exception {
                if(distributeFile.contains(value)){
                    out.collect(value);
                }
            }
        }).print();

    }
}

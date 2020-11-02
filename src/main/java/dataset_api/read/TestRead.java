package yux.dataset_api.read;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Test;

public class TestRead extends AbstractTestBase{

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, Integer>> source =
                env.readCsvFile("file:///Users/yuxiang/codes/flink_test/src/main/java/yux/dataset_api/read/text.csv")
                .includeFields("10010")
                .types(Integer.class, Integer.class);

        source.print();

        // 级联读文件夹，会读所有的子文件夹的文件
        Configuration config = new Configuration();
        config.setBoolean("recursive.file.enumeration", true);
        env.readTextFile("").withParameters(config);



    }

}

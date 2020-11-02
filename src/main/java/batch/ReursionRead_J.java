package batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

public class ReursionRead_J {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        Configuration conf = new Configuration();
        conf.setBoolean("recursive.file.enumeration=true", true);

        DataSet<String> dset = env
                .readTextFile("/Users/yuxiang/Downloads")
                .withParameters(conf);

        dset.print();
    }
}

package table_sql;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.BatchTableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.table.api.EnvironmentSettings;
import org.junit.Before;
import org.junit.Test;

public class CreateTableEnv extends AbstractTestBase {


    StreamExecutionEnvironment senv;
    ExecutionEnvironment env;

    @Before
    public void createExecuteEnv(){

        senv = StreamExecutionEnvironment.getExecutionEnvironment();
        env = ExecutionEnvironment.getExecutionEnvironment();
    }

    @Test
    public void createOlderPlannerBatchQuery(){

        BatchTableEnvironment bfTableEnv = BatchTableEnvironment.create(env);
    }

    @Test
    public void createOlderPlannerStreamQuery(){

        EnvironmentSettings fsSetting =
                EnvironmentSettings.newInstance().useOldPlanner().inStreamingMode().build();
        StreamTableEnvironment sTableEnv = StreamTableEnvironment.create(senv, fsSetting);
    }

    @Test
    public void createBlinkBatchPlanner(){
        EnvironmentSettings fsSetting =
                EnvironmentSettings.newInstance().useBlinkPlanner().inBatchMode().build();
        TableEnvironment bbTableEnv = TableEnvironment.create(fsSetting);
    }

    @Test
    public void createBlinkStreamPlanner(){
        EnvironmentSettings bsSettings =
                EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(senv, bsSettings);
    }
}

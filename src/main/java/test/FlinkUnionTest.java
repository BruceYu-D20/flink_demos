package yux.test;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FlinkUnionTest extends AbstractTestBase {

    StreamExecutionEnvironment env;

    @Before
    public void createEnv(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
    }

    @Test
    public void testMethod_1(){
        DataStreamSource<Integer> source = env.fromElements(1, 2, 3, 4, 5);
        source.print();

    }

    @After
    public void runJob() throws Exception {
        env.execute("flink union test");
    }
}

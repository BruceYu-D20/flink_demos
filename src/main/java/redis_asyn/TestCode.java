package redis_asyn;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.junit.Test;

import java.util.List;

public class TestCode {

    @Test
    public void testMysqlSource() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<List<String>> mysqlSource = env.addSource(new MysqlMsisdnsSource());
        
        mysqlSource.map(new MapFunction<List<String>, Object>() {
            @Override
            public Object map(List<String> msisdns) throws Exception {

                for(String msisdn: msisdns){
                    System.out.println(msisdn);
                }
                return null;
            }
        });
        env.execute("run job");
    }

}

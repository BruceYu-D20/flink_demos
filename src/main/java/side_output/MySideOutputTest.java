package yux.side_output;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class MySideOutputTest extends AbstractTestBase {

    StreamExecutionEnvironment env;
    ExecutionEnvironment env1;

    @Before
    public void createEnv(){
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env1 = ExecutionEnvironment.getExecutionEnvironment();
    }

    @After
    public void runJob() throws Exception {
        env.execute("side output test");
    }

    @Test
    public void testSideOutputData(){

        final OutputTag<String> info = new OutputTag<String>("info"){};
        final OutputTag<String> warn = new OutputTag<String>("warn"){};
        final OutputTag<String> error = new OutputTag<String>("error"){};

        DataStreamSource<String> source = env.socketTextStream("localhost", 38888);

        SingleOutputStreamOperator<String> process = source.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String value, Context ctx, Collector<String> out) throws Exception {

                String[] splited = value.split(",");
                if (splited[0].toLowerCase().equals("info")) {
                    ctx.output(info, splited[0] + "_mysplit" + splited[1]);
                } else if (splited[0].toLowerCase().equals("warn")) {
                    ctx.output(warn, splited[0] + "_mysplit" + splited[1]);
                } else if (splited[0].toLowerCase().equals("warn")) {
                    ctx.output(error, splited[0] + "_mysplit" + splited[1]);
                } else {
                    out.collect(value);
                }
            }
        });

        // 获取到info流的数据
        process.getSideOutput(info).print();
    }

    @Test
    public void handleParams() throws IOException {
        String[] args = new String[]{};
        // 从文件中获取
        ParameterTool tools0 = ParameterTool.fromPropertiesFile("params.properties");
        // 从参数中获取
        ParameterTool tools1 = ParameterTool.fromArgs(args);

        // 获取参数
        tools1.getRequired("input");
        tools1.get("output", "myDefaultValue");
        tools1.getLong("expectedCount", -1L);
        tools1.getNumberOfParameters();

        // 设置Globle参数
        env.getConfig().setGlobalJobParameters(tools1);

    }

    /**
     * 获取全局参数的实例
     */
    class GetGlobleParam extends RichMapFunction<String, String> {

        @Override
        public String map(String value) throws Exception {
            ParameterTool tools =
                    (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
            tools.getRequired("input");
            return null;
        }
    }
}

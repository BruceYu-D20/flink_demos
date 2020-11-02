package yux.dataset_api;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.test.util.AbstractTestBase;
import org.junit.Before;
import org.junit.Test;
import java.util.ArrayList;
import java.util.List;

public class FlinkJoinTest extends AbstractTestBase {

    ExecutionEnvironment env;
    List<Tuple2<Integer, String>> input1;
    List<Tuple2<Integer, String>> input2;

    @Before
    public void createEnv(){
        env = ExecutionEnvironment.getExecutionEnvironment();

        // 数据构造
        input1 = new ArrayList<>();
        input1.add(new Tuple2<Integer, String>(1, "beijing"));
        input1.add(new Tuple2<>(2, "nanjing"));
        input1.add(new Tuple2<>(3, "chongqing"));

        input2 = new ArrayList<>();
        input2.add(new Tuple2<>(1, "liming"));
        input2.add(new Tuple2<>(2, "zhangsan"));
        input2.add(new Tuple2<>(2, "wangwu"));
        input2.add(new Tuple2<>(3, "zhaoliu"));
        input2.add(new Tuple2<>(4, "jia"));
        input2.add(new Tuple2<>(1, "yi"));
        input2.add(new Tuple2<>(2, "bing"));
        input2.add(new Tuple2<>(5, "ding"));
    }

    @Test
    public void testJoin() throws Exception {

        DataSource<Tuple2<Integer, String>> source1 = env.fromCollection(input1);
        DataSource<Tuple2<Integer, String>> source2 = env.fromCollection(input2);

        DataSet<Tuple3<Integer, String, String>> joinedInfo =
                source1.join(source2, JoinOperatorBase.JoinHint.BROADCAST_HASH_FIRST)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        return new Tuple3<>(first.f0, first.f1, second.f1);
                    }
                });

        joinedInfo.print();
    }

    @Test
    public void testLeftJoin() throws Exception {
        DataSource<Tuple2<Integer, String>> source1 = env.fromCollection(input1);
        DataSource<Tuple2<Integer, String>> source2 = env.fromCollection(input2);

        DataSet<Tuple3> fullJoinedDS = source1.fullOuterJoin(source2)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple2<Integer,String>, Tuple2<Integer,String>, Tuple3>() {
                    @Override
                    public Tuple3 join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if(first == null){
                            return new Tuple3<>(second.f0, "null", second.f1);
                        }else if(second == null){
                            return new Tuple3<>(first.f0, first.f1, "null");
                        }else{
                            return new Tuple3(first.f0, first.f1, second.f1);
                        }
                    }
                }).returns(Types.TUPLE(Types.INT, Types.STRING, Types.STRING));

        fullJoinedDS.print();
    }

}

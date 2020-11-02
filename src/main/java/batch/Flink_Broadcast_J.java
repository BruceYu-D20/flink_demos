package batch;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

public class Flink_Broadcast_J {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, String, Integer>> ds1 =
                env.fromElements(Tuple3.of(1, "zhangsan", 1),
                        Tuple3.of(2, "lisi", 1),
                        Tuple3.of(3, "wangwu", 2),
                        Tuple3.of(4, "zhaoliu",4));


        DataSet<Tuple2<Integer, String>> ds2 = env.fromElements(
                Tuple2.of(1, "SH"),
                Tuple2.of(2, "BJ"),
                Tuple2.of(3, "CQ")
        );


        DataSet<List<Flink_Join_J.PeopleStay>> pstays = ds1.map(new RichMapFunction<Tuple3<Integer, String, Integer>, List<Flink_Join_J.PeopleStay>>() {

            List<Tuple2<Integer, String>> cinfos = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                cinfos = getRuntimeContext().getBroadcastVariable("cinfos");
            }

            @Override
            public List<Flink_Join_J.PeopleStay> map(Tuple3<Integer, String, Integer> pinfos) throws Exception {
                List<Flink_Join_J.PeopleStay> pstayList = new ArrayList();
                for (Tuple2<Integer, String> cinfo : cinfos) {
                    if (cinfo.f0 == pinfos.f2) {
                        pstayList.add(new Flink_Join_J.PeopleStay(pinfos.f0, pinfos.f1, cinfo.f1));
                    }
                }

                return pstayList;
            }
        }).withBroadcastSet(ds2, "cinfos");


        System.out.println("--");
        DataSet<Flink_Join_J.PeopleStay> c = pstays.flatMap(new FlatMapFunction<List<Flink_Join_J.PeopleStay>, Flink_Join_J.PeopleStay>() {
            @Override
            public void flatMap(List<Flink_Join_J.PeopleStay> peopleStays, Collector<Flink_Join_J.PeopleStay> out) throws Exception {
                for (Flink_Join_J.PeopleStay a : peopleStays) {
                    out.collect(a);
                }
            }
        });

        c.print();

    }
}

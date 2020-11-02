package batch;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.operators.join.JoinFunctionAssigner;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;


public class Flink_Join_J {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple3<Integer, String, Integer>> ds1 =
                env.fromElements(Tuple3.of(1, "zhangsan", 1),
                    Tuple3.of(2, "lisi", 1),
                    Tuple3.of(3, "wangwu", 2),
                    Tuple3.of(4, "zhaoliu",4));

        DataSet<Tuple2<Integer, String>> ds2 = env.fromElements(Tuple2.of(1, "SH"),
                Tuple2.of(2, "BJ"),
                Tuple2.of(3, "CQ"));

        /**
         * 1. inner join
         */
        JoinOperator.DefaultJoin<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>> personInfo =
                ds1.join(ds2).where(2).equalTo(0);

        System.out.println("-----------inner join-----------");


        // join后数据处理1：用map
        DataSet<PeopleStay> ds3 =
            personInfo.map(new MapFunction<Tuple2<Tuple3<Integer,String,Integer>,Tuple2<Integer,String>>, PeopleStay>() {
                @Override
                public PeopleStay map(Tuple2<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>> peopleInfo) throws Exception {
                    return new PeopleStay(peopleInfo.f0.f0, peopleInfo.f0.f1, peopleInfo.f1.f1);
                }
            });
        // join后数据处理2：
        DataSet<PeopleStay> ds4 =personInfo.with(new peoStayJoinFunc());

        ds3.print();
        System.out.println("--");
        ds4.print();

        /**
         * 左外连接 leftOuterJoin
         * 右外连接 rightOuterJoin
         * 全连接 fullOuterJoin
         */
        System.out.println("-----------leftOuter Join Or rightOuter Join Or fullOuter Join-----------");
        JoinFunctionAssigner<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>> personLeft =
                ds1.leftOuterJoin(ds2).where(2).equalTo(0);
        DataSet<PeopleStay> pLeftds =
            personLeft.with(new JoinFunction<Tuple3<Integer, String, Integer>, Tuple2<Integer, String>, PeopleStay>() {
                @Override
                public PeopleStay join(Tuple3<Integer, String, Integer> pInfo,
                                       Tuple2<Integer, String> cinfo) throws Exception {
                    if (cinfo == null) {
                        return new PeopleStay(pInfo.f0, pInfo.f1, "NO_CITY");
                    }
                    return new PeopleStay(pInfo.f0, pInfo.f1, cinfo.f1);
                }
            });
        pLeftds.print();

    }

    public static class peoStayJoinFunc
            implements JoinFunction<Tuple3<Integer,String,Integer>,Tuple2<Integer,String>, PeopleStay>{

        @Override
        public PeopleStay join(Tuple3<Integer, String, Integer> personInfo, Tuple2<Integer, String> cityInfo) throws Exception {
            return new PeopleStay(personInfo.f0, personInfo.f1, cityInfo.f1);
        }
    }



    public static class PeopleStay{
        public int id;
        public String name;
        public String city;

        public PeopleStay(int id, String name, String city){
            this.name = name;
            this.id = id;
            this.city = city;
        }

        @Override
        public String toString() {
            return "id " + id + " name " + name + " city " + city;
        }
    }
}

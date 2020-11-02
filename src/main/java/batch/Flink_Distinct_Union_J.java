package batch;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.DistinctOperator;
import org.apache.flink.api.java.tuple.Tuple3;

import java.util.ArrayList;

public class Flink_Distinct_Union_J {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<Tuple3<Long, String, Integer>> dset =
                env.fromElements(Tuple3.of(1L, "zhangyi", 10),
                        Tuple3.of(2L, "zhanger", 11),
                        Tuple3.of(2L, "zhangsan", 11));

        dset.distinct(0).print();
        System.out.println("---");

        ArrayList list1 = new ArrayList<Tuple3<Long, String, Integer>>();
        list1.add(Tuple3.of(3L, "zhangsi", 13));
        list1.add(Tuple3.of(4L, "zhangwu", 14));
        DataSet<Tuple3<Long, String, Integer>> dset1 = env.fromCollection(list1);

        DataSet<Tuple3<Long, String, Integer>> dsetUnion = dset.union(dset1);

        dsetUnion.print();

    }
}

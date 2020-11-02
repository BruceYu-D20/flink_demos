package yux.broadcast.UserPurchaseBehavior;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import yux.broadcast.UserPurchaseBehavior.pojo.Config;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

public class MysqlConfSource extends RichSourceFunction<HashMap<String, Config>> implements CheckpointedFunction {

    private String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/test";
    private String user = "root";
    private String passwd = "root";
    private Integer secondInterval = 1;

    private Connection conn = null;
    private PreparedStatement pst = null;

    private boolean isRunning = true;

    /**
     * 托管状态
     */
    private transient ListState<HashMap<String, Config>> checkPointList;

    /**
     * 原始状态
     */
    private List<HashMap<String, Config>> listBufferElements;

    public MysqlConfSource(){}

    public MysqlConfSource(String jdbcUrl, String user, String passwd, Integer secondInterval) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.passwd = passwd;
        this.secondInterval = secondInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection(jdbcUrl, user, passwd);
        listBufferElements = new ArrayList<>();

    }

    @Override
    public void run(SourceContext<HashMap<String, Config>> out) throws Exception {
        String staticStatusSql =
                "SELECT channel,registerDate,historyPurchaseTimes,maxPurchasePathLength " +
                        "FROM user_purchase_conf";

        pst = conn.prepareStatement(staticStatusSql);

        HashMap<String, Config> mysqlData = new HashMap();

        while (isRunning){

            System.out.println("im in");

            ResultSet rs = pst.executeQuery();

            while(rs.next()){
                String channel = rs.getString("channel");
                String registerDate = rs.getString("registerDate");
                int historyPurchaseTimes = rs.getInt("historyPurchaseTimes");
                int maxPurchasePathLength = rs.getInt("maxPurchasePathLength");
                Config config = new Config(channel, registerDate, historyPurchaseTimes, maxPurchasePathLength);
                mysqlData.put(channel, config);
            }

            // 如果有状态，说明运行过，比较两次的状态，如果相等，就不发送；如果不相同，发送并改状态
            System.out.println("listBufferElements是不是空：" + listBufferElements.isEmpty());
            if(!listBufferElements.isEmpty()){
                System.out.println("---listBufferElements的值---");
                System.out.println(listBufferElements);
                System.out.println("---checkpoint的值---");
                System.out.println(listBufferElements.get(0));
                System.out.println("---数据的值---");
                System.out.println(mysqlData);
                System.out.println("---是否相等---");
                System.out.println(listBufferElements.get(0).equals(mysqlData));

                if(listBufferElements.get(0).equals(mysqlData)){
                    // listbuffer不为空，且和之前查询的内容相同，就不查询
                    System.out.println("相等了 这次不发送");
                    System.out.println();
                }else{
                    // listbuffer不为空，且和之前查询的内容不同，查询
                    System.out.println("不相等，这次要发送");
                    System.out.println();
//                    listBufferElements.set(0, mysqlData);
                    out.collect(mysqlData);
                }
            }else {
                System.out.println();
                System.out.println("---第一次查询---");
                System.out.println("---数据的值---");
                System.out.println(mysqlData);
                System.out.println();
                // listbuffer为空，直接查询
                listBufferElements.add(mysqlData);
                out.collect(mysqlData);
            }

            Thread.sleep(secondInterval * 1000);
        }
    }

    @Override
    public void cancel() {

        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        super.close();
        if(conn != null){
            conn.close();
        }
        if(pst != null){
            pst.close();
        }
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        System.out.println("snapshotState--------------------");
        checkPointList.clear();
        checkPointList.add(listBufferElements.get(0));
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {

        System.out.println("initializeState---------------------");
        ListStateDescriptor<HashMap<String, Config>> configurationDesc =
                new ListStateDescriptor<>("conf_des",
                        TypeInformation.of(new TypeHint<HashMap<String, Config>>() {}));
        checkPointList = context.getOperatorStateStore().getListState(configurationDesc);
        if(context.isRestored()){
            Iterator<HashMap<String, Config>> itr = checkPointList.get().iterator();
            while(itr.hasNext()){
                listBufferElements.set(0, itr.next());
            }
        }
    }

}

package broadcast.mysqlConfBst;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;

public class MysqlSource extends RichSourceFunction<HashMap<String, Tuple2<String, Integer>>> {

    private String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/test";
    private String user = "root";
    private String passwd = "root";
    private Integer secondInterval = 5;

    private Connection conn = null;
    private PreparedStatement pst1 = null;
    private PreparedStatement pst2 = null;

    private boolean isRunning = true;
    private boolean isFirstTime = true;

    public MysqlSource(){}

    public MysqlSource(String jdbcUrl, String user, String passwd,Integer secondInterval) {
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

    }

    @Override
    public void run(SourceContext<HashMap<String, Tuple2<String, Integer>>> out) throws Exception {
        String staticStatusSql = "SELECT up_status FROM my_status";
        String sql = "SELECT id,name,age FROM account";

        pst1 = conn.prepareStatement(staticStatusSql);
        pst2 = conn.prepareStatement(sql);

        HashMap<String, Tuple2<String, Integer>> mysqlData = new HashMap();

        while (isRunning){

            ResultSet rs1 = pst1.executeQuery();
            Boolean isUpdateStatus = false;
            while(rs1.next()){
                isUpdateStatus = (rs1.getInt("up_status") == 1);
            }
            System.out.println();
            System.out.println("isUpdateStatus:  " + isUpdateStatus + " isFirstTime: " + isFirstTime);

            if(isUpdateStatus || isFirstTime){
                ResultSet rs2 = pst2.executeQuery();
                while(rs2.next()){
                    int id = rs2.getInt("id");
                    String name = rs2.getString("name");
                    int age = rs2.getInt("age");
                    mysqlData.put(id + "", new Tuple2<String, Integer>(name, age));
                }
                isFirstTime = false;
                System.out.println("我查了一次mysql，数据是： " + mysqlData);
                out.collect(mysqlData);
            }else{
                System.out.println("这次没查mysql");
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
        if(pst1 != null){
            pst1.close();
        }
        if(pst2 != null){
            pst2.close();
        }
    }
}

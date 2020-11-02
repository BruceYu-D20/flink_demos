package redis_asyn;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class MysqlMsisdnsSource extends RichSourceFunction<List<String>>{

    private String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/test";
    private String user = "root";
    private String passwd = "root";
    private Integer secondInterval = 5;

    private Connection conn = null;
    private PreparedStatement pst1 = null;
    private PreparedStatement pst2 = null;

    private boolean isRunning = true;
    private boolean isFirstTime = true;

    public MysqlMsisdnsSource(){}

    public MysqlMsisdnsSource(String jdbcUrl, String user, String passwd, Integer secondInterval) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.passwd = passwd;
        this.secondInterval = secondInterval;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection(jdbcUrl, user, passwd);
    }

    @Override
    public void run(SourceContext<List<String>> out) throws Exception {

        String tableIsChangedSQL =
                "select TABLE_NAME as name,cast(ifnull(UPDATE_TIME,CREATE_TIME) as char(22)) as version " +
                "from information_schema.tables " +
                "where TABLE_SCHEMA=database() and TABLE_TYPE='BASE TABLE' and TABLE_NAME='msisdns'";
        String msisdnSQL = "SELECT msisdn FROM msisdns";

        pst1 = conn.prepareStatement(tableIsChangedSQL);
        pst2 = conn.prepareStatement(msisdnSQL);

        String lastedVersion = "-1";

        while(isRunning){
            ResultSet tableIsChangedRS = pst1.executeQuery(tableIsChangedSQL);
            String currentVersion = "-2";
            while (tableIsChangedRS.next()){
                currentVersion = tableIsChangedRS.getString("version");
            }

            if(isFirstTime == true || !currentVersion.equals(lastedVersion)){
                List<String> msisdns = new ArrayList<>();
                ResultSet msisdnsRS = pst2.executeQuery(msisdnSQL);
                while(msisdnsRS.next()){
                    msisdns.add(msisdnsRS.getString("msisdn"));
                }
                // 发数据
                out.collect(msisdns);
                //改状态
                lastedVersion = currentVersion;
                isFirstTime = false;
                msisdnsRS.close();
            }
            tableIsChangedRS.close();
            Thread.sleep(secondInterval * 1000);

        }

    }

    @Override
    public void cancel() {

        isRunning = false;
    }

    @Override
    public void close() throws Exception {
        if(pst1 != null){
            pst1.close();
        }
        if(pst2 != null){
            pst2.close();
        }
        if(conn != null){
            conn.close();
        }
    }
}

package yux.broadcast.mysqlConfBst;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class MysqlSink extends RichSinkFunction<Tuple3<Integer, String, Integer>> {

    private String jdbcUrl = "jdbc:mysql://127.0.0.1:3306/test";
    private String user = "root";
    private String passwd = "root";

    private Connection conn = null;
    private PreparedStatement pst = null;
    private PreparedStatement pstIst = null;

    public MysqlSink(){}

    public MysqlSink(String jdbcUrl, String user, String passwd) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.passwd = passwd;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        conn = DriverManager.getConnection(this.jdbcUrl, this.user, this.passwd);
    }

    @Override
    public void invoke(Tuple3<Integer, String, Integer> student, Context context) throws Exception {
        String creatTableSQL = "CREATE TABLE IF NOT EXISTS test.mysql_sink_test" +
                "(" +
                "id INT," +
                "stu_name VARCHAR(25)," +
                "age INT" +
                ")";

        String insertSQL =
                "INSERT INTO test.mysql_sink_test(id,stu_name,age) VALUES(?,?,?)";

        // 如果表不存在，建表
        pst = conn.prepareStatement(creatTableSQL);
        pst.execute();

        // 插入语句
        pstIst = conn.prepareStatement(insertSQL);
        pstIst.setInt(1, student.f0);
        pstIst.setString(2, student.f1);
        pstIst.setInt(3, student.f2);
        int result = pstIst.executeUpdate();
        System.out.println(result);
    }



    @Override
    public void close() throws Exception {
        super.close();
        pst.close();
        pstIst.close();
        conn.close();
    }
}

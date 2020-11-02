package watermarks_lateness;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import DBPool.MyDBPoolUtils;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

public class MysqlAsynFunction extends RichAsyncFunction<String, String> {

    static ComboPooledDataSource dataSource;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/test");
        try {
            dataSource.setDriverClass("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
        dataSource.setUser("root");
        dataSource.setPassword("root");
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

        Connection conn = dataSource.getConnection();
        PreparedStatement pst = conn.prepareStatement("SELECT id,name,age FROM account WHERE id=1");
        ResultSet rs = pst.executeQuery();
        List<String> info = new ArrayList<>();
        if(rs.next()){
            int id = rs.getInt("id");
            String name = rs.getString("name");
            info.add(input + " : " + id + "_" + name);
            System.out.println(input + " : " + id + "_" + name);
        }
        resultFuture.complete(info);

    }
}

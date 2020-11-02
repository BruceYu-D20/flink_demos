package DBPool;

import com.mchange.v2.c3p0.ComboPooledDataSource;

import java.beans.PropertyVetoException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class MyDBPoolUtils {

    //通过标识名来创建相应连接池
    static ComboPooledDataSource dataSource;

    static {
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


    //从连接池中取用一个连接
    public static Connection getConnection() throws Exception {
        try {
            return dataSource.getConnection();
        } catch (Exception e) {
            throw new Exception("数据库连接出错!", e);
        }
    }
    //释放连接回连接池
    public static void close(Connection conn, PreparedStatement pst, ResultSet rs) throws Exception {
        if(rs!=null){
            try {
                rs.close();
            } catch (SQLException e) {
                throw new Exception("数据库连接关闭出错!", e);
            }
        }
        if(pst!=null){
            try {
                pst.close();
            } catch (SQLException e) {
                throw new Exception("数据库连接关闭出错!", e);
            }
        }

        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                throw new Exception("数据库连接关闭出错!", e);
            }
        }
    }
}

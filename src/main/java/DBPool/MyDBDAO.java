package DBPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

public class MyDBDAO {

    /**
     *  增加，删除，修改  
     */
    public static void insertOrDeleteOrUpdate(String sql){
        try {
            Connection connection = MyDBPoolUtils.getConnection();
            PreparedStatement pst = connection.prepareStatement(sql);
            int execute = pst.executeUpdate();
            System.out.println("执行语句：" + sql + "," + execute + "行数据受影响");
            MyDBPoolUtils.close(connection, pst, null);
        } catch (Exception e) {
            System.out.println("异常提醒：" + e);
        }
    }

    /**
     *  查询，返回结果集  
     */
    public static List<Map<String, Object>> select(String sql) {
        List<Map<String, Object>> returnResultToList = null;
        try {
            Connection connection = MyDBPoolUtils.getConnection();
            PreparedStatement pst = connection.prepareStatement(sql);
            ResultSet resultSet = pst.executeQuery();
            MyDBPoolUtils.close(connection, pst, resultSet);
        } catch (Exception e) {

            System.out.println("异常提醒：" + e);
        }
        return returnResultToList;
    }

}

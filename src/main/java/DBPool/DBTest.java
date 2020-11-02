package DBPool;


import java.sql.*;
import java.util.List;
import java.util.Map;

public class DBTest {

    public static void main(String[] args) throws Exception {

        testc3P0();
    }

    public static void testc3P0() throws Exception {
        Connection conn = DBPool.MyDBPoolUtils.getConnection();

        PreparedStatement pst = conn.prepareStatement("SELECT id,name FROM account");

        ResultSet rs = pst.executeQuery();

        while(rs.next()){
            System.out.println(rs.getInt("id") + "__" + rs.getString("name"));
        }
    }
}

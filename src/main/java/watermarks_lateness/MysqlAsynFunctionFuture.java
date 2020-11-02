package watermarks_lateness;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Collections;
import java.util.concurrent.*;

public class MysqlAsynFunctionFuture extends RichAsyncFunction<String, String> {

    static ComboPooledDataSource dataSource;

    private transient ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl("jdbc:mysql://127.0.0.1:3306/test");
        try {
            dataSource.setDriverClass("com.mysql.jdbc.Driver");
        } catch (Exception e) {
            e.printStackTrace();
        }
        dataSource.setUser("root");
        dataSource.setPassword("root");

        executorService = Executors.newFixedThreadPool(30);
    }

    @Override
    public void close() throws Exception {
        dataSource.close();
    }

    @Override
    public void asyncInvoke(String input, ResultFuture<String> resultFuture) throws Exception {

        /**
         * 用executorService实现
         */
        executorService.submit(new Runnable() {

            @Override
            public void run() {
                try{
                    Connection conn = dataSource.getConnection();
                    PreparedStatement pst = conn.prepareStatement("SELECT id,name,age FROM account WHERE id=" + input);
                    ResultSet rs = pst.executeQuery();
                    StringBuilder info = new StringBuilder("");
                    if(rs.next()){
                        int id = rs.getInt("id");
                        String name = rs.getString("name");
                        info.append(id).append("_").append(name);
                    }
                    resultFuture.complete(Collections.singleton(info.toString()));
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
        });

        /**
         * 用CompletableFuture实现
         */
        /**
         *
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @SneakyThrows
            @Override
            public String get() {
                Connection conn = dataSource.getConnection();
                PreparedStatement pst = conn.prepareStatement("SELECT id,name,age FROM account WHERE id=" + input);
                ResultSet rs = pst.executeQuery();
                StringBuilder info = new StringBuilder("");
                if(rs.next()){
                    int id = rs.getInt("id");
                    String name = rs.getString("name");
                    info.append(id).append("_").append(name);
                }
                return info.toString();
            }
        }).thenAccept((String dbResult) -> resultFuture.complete(Arrays.asList(dbResult)));
         */
    }
}

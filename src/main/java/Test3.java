package yux;

import org.apache.commons.collections.map.HashedMap;
import yux.broadcast.mysqlConfBst.MysqlSink;

import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class Test3 {

    public static void main(String[] args) {
        Set<Integer> tree = new TreeSet<>();

        for(int i = 0; i <= 100; i ++){
            tree.add((int) (Math.random() * 100));
        }

//        for(Integer leef: tree){
//            System.out.println(leef);
//        }

        for(MyService value: MyService.values()){
            System.out.println(value);
            System.out.println(value.name());
            System.out.println(value.getName());
            System.out.println();
        }
    }
}

enum MyService{
    All("all"),
    ST_MCC("st_mcc"),
    ST_UE("st_ue");

    private String name;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    MyService(String name) {
        this.name = name;
    }
}

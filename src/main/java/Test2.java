package yux;

import java.util.HashMap;

public class Test2 {

    public static HashMap<String, String> mymap;

    static {
        System.out.println("我进了静态块");
        mymap = new HashMap<>();
        mymap.put("1", "1");
        System.out.println(mymap);
    }


    public Test2(){
        System.out.println("我进了构造器");
    }

}

package yux.test;

import yux.broadcast.UserPurchaseBehavior.pojo.Config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class Mytest {

    public static void main(String[] args) {

        HashMap<String, Config> map1 = new HashMap<>();
        HashMap<String, Config> map2 = new HashMap<>();

        Config conf1 = new Config("a","a",1,1);
        Config conf2 = new Config("b","b",1,1);
        Config conf3 = new Config("c","c",1,1);
        Config conf4 = new Config("d","d",1,1);
        Config conf5 = new Config("e","e",1,1);

        map1.put(conf1.getChannel(), conf1);
        map1.put(conf2.getChannel(), conf2);
        map1.put(conf3.getChannel(), conf3);
        map1.put(conf4.getChannel(), conf4);
        map1.put(conf5.getChannel(), conf5);

        map2.put(conf5.getChannel(), conf5);
        map2.put(conf1.getChannel(), conf1);
        map2.put(conf2.getChannel(), conf2);
        map2.put(conf4.getChannel(), conf4);
        map2.put(conf3.getChannel(), conf3);

        System.out.println(map1.equals(map2));

        List<String> strings = new ArrayList<>();
        System.out.println(strings.get(0) == null);


    }
}

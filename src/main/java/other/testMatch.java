package yux.other;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class testMatch {

    public static void main(String[] args) {

        String sql = "CREATE TABLE MyTable(name varchar,channel varchar,pv int,xctime bigint) WITH (zookeeperQuorum ='172.16.8.198:2181/kafka',offsetReset ='latest',topic ='nbTest1',parallelism ='1');";

        String PATTERN_STR = "(?i)create\\s+table\\s+(\\S+)\\s*\\((.+)\\)\\s*with\\s*\\((.+)\\)";

        Pattern PATTERN = Pattern.compile(PATTERN_STR);

        Matcher matcher = PATTERN.matcher(sql);

        System.out.println(matcher.find());
        System.out.println(matcher.group(2));

        String fields = matcher.group(2);
        String[] splits = fields.split(",");

        for(String fieldRow: splits){
            String[] fieldsInfoArr = fieldRow.split("\\s");
            for(String fieldInfoArr: fieldsInfoArr){
                System.out.println(fieldInfoArr);
            }
            System.out.println();
        }

    }
}

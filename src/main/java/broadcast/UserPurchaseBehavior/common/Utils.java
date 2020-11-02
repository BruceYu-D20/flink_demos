package yux.broadcast.UserPurchaseBehavior.common;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class Utils {

    public static ParameterTool parameterCheck(String[] args){

        ParameterTool param = ParameterTool.fromArgs(args);

        if(!param.has(Params.SERVER_URL)){
            System.err.println("dont have serverUrl");
        }

        if(!param.has(Params.TOPICS)){
            System.err.println("dont have topics");
        }

        return param;
    }

    public static Set<String> getTopics(String topicParam){

        Set<String> topics = new HashSet<>();
        String[] splited = topicParam.split(",");
        for(String topic: splited){
            topics.add(topic);
        }

        return topics;
    }


}

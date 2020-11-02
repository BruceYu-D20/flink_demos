package yux.broadcast.UserPurchaseBehavior.common;

public class Params {

    public static String SERVER_URL = "serverurl";

    public static String TOPICS = "topics";

    /**
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12 09:27:11"}
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"ADD_TO_CART","eventTime":"2018-06-12 09:43:18"}
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"VIEW_PRODUCT","eventTime":"2018-06-12 09:27:11"}
     * {"userId":"d8f3368aba5df27a39cbcfd36ce8084f","channel":"APP","eventType":"PURCHASE","eventTime":"2018-06-12 09:30:28"}
     */
    public static String USER_EVENT_SCHEMA = "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"eventTime\",\"type\":\"long\"},{\"name\":\"userId\",\"type\":\"string\"},{\"name\":\"channel\",\"type\":\"string\"},{\"name\":\"eventType\",\"type\":\"string\"}]}";
}

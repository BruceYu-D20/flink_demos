package yux.pulsar;

public class Utils {

    public static final String SCHEMA_NAME = "test_schema";

    public static String MY_SCHEMA = "{\"type\":\"record\",\"name\":\"test_pulsar\",\"fields\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";

    public static String PULSAR_HDFS_SCHEMA = "{\"type\":\"record\",\"name\":\"Test\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"fname\",\"type\":\"string\"}]}";

    public static String PULSAR_USER_SCHEMA = "{\"type\":\"record\",\"name\":\"Test1\",\"fields\":[{\"name\":\"id\",\"type\":\"int\"},{\"name\":\"fname\",\"type\":\"string\"},{\"name\":\"age\",\"type\":\"int\"}]}";

}

package connector.pulsar;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class PulsarOptions {

    private PulsarOptions() {}

    public static final ConfigOption<String> SERVICEURL = ConfigOptions
            .key("pulsar.serviceUrl")
            .stringType()
            .noDefaultValue()
            .withDescription("required url for pulsar");

    public static final ConfigOption<String> SUBSCRIPTIONTYPE = ConfigOptions
            .key("pulsar.subscriptionType")
            .stringType()
            .noDefaultValue()
            .withDescription("subscription type for pulsar");

    public static final ConfigOption<String> SUBSCRIPTIONNAME = ConfigOptions
            .key("pulsar.subscriptionName")
            .stringType()
            .noDefaultValue()
            .withDescription("subscription name for pulsar");

    public static final ConfigOption<String> TOPIC = ConfigOptions
            .key("topic")
            .stringType()
            .noDefaultValue()
            .withDescription("which topic to subscribe");

    public static final ConfigOption<String> SUBSCRIPTIONINITIALPOS = ConfigOptions
            .key("pulsar.subscriptionInitialPosition")
            .stringType()
            .noDefaultValue()
            .withDescription("consume initial position");

    public static final ConfigOption<String> TABLESCHEMA = ConfigOptions
            .key("pulsar.tableSchema")
            .stringType()
            .noDefaultValue()
            .withDescription("table schema for topic");
}

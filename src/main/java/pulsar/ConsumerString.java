package yux.pulsar;

import org.apache.pulsar.client.api.*;

public class ConsumerString {

    public static void main(String[] args) throws Exception {

        PulsarClient client = PulsarClient
                .builder()
                .serviceUrl("pulsar://172.16.108.110:6650")
                .build();

        Consumer<String> consumer = client.newConsumer(Schema.STRING)
                .topic("persistent://public/default/par_topic")
                .subscriptionName("name3")
                .subscriptionType(SubscriptionType.Failover)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscribe();

        while (true){
            Message<String> msg = consumer.receive();
            System.out.println(msg.getValue());
            consumer.acknowledge(msg);
        }
    }
}

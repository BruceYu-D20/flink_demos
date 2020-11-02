package yux.pulsar;


import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

/**
 *
 */
public class PulsarProducder_String {

    public static void main(String[] args) throws Exception {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://172.16.108.110:6650")
                .build();

        Producer producer = client.newProducer(Schema.STRING)
                .topic("persistent://public/default/par_topic")
                .batchingMaxMessages(10)
                .create();


        String[] names = new String[]{"jack", "tom", "jerry"};
        Integer[] ages = new Integer[]{10, 18, 3};
        int i = 0;

        while(true){

            i += 1;
            int id = ages[(int)(Math.random() * 3)];
            String fname = names[(int)(Math.random() * 3)];

            String message = i + "," + id + "," + fname;
            System.out.println(message);
            producer.sendAsync(message);

            Thread.sleep(200);
        }


    }
}

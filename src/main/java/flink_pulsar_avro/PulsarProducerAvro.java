package flink_pulsar_avro;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.schema.*;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.util.Collections;

public class PulsarProducerAvro {

    public static void main(String[] args) throws PulsarClientException, InterruptedException {

        PulsarClient client = PulsarClient.builder()
                .serviceUrl("pulsar://172.16.108.110:6650")
                .build();

        Producer<UserPOJO> producer =
//                client.newProducer(Schema.AVRO(yx.pojo.User.class))
                client.newProducer(Schema.AVRO(UserPOJO.class))
                .topic("test_avro_user_pojo")
                .create();

        while(true){

            String[] names = {"zhangsan", "lisi", "wangwu"};
            String[] favorateColors = {"red", "yellow", "green"};
            int[] favorateNumbers = {1, 2, 3};
            int index = (int) (Math.random() * 3);


//            User user = User.newBuilder()
//                    .setName(names[index])
//                    .setFavoriteColor(favorateColors[index]).setFavoriteNumber(favorateNumbers[index])
//                    .build();

            UserPOJO user = new UserPOJO();
            user.setName(names[index]);
            user.setFavorite_color(favorateColors[index]);
            user.setFavorite_number(favorateNumbers[index]);

            producer.send(user);

            Thread.sleep(1000);

        }

    }
}

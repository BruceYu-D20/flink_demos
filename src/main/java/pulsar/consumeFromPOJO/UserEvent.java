package yux.pulsar.consumeFromPOJO;


import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class UserEvent {

    private String name;
    private int age;

}

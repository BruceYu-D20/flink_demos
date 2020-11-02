package yux.broadcast.UserPurchaseBehavior.pojo;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@Data
@ToString
public class Config {

    private String channel;
    private String registerDate;
    private Integer historyPurchaseTimes;
    private Integer maxPurchasePathLength;

    public Config() {
    }

    public Config(String channel, String registerDate, Integer historyPurchaseTimes, Integer maxPurchasePathLength) {
        this.channel = channel;
        this.registerDate = registerDate;
        this.historyPurchaseTimes = historyPurchaseTimes;
        this.maxPurchasePathLength = maxPurchasePathLength;
    }
}

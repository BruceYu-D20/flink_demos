package flink_pulsar_avro;

public class UserPOJO {

    private String name;
    private Integer favorite_number;
    private String favorite_color;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getFavorite_number() {
        return favorite_number;
    }

    public void setFavorite_number(Integer favorite_number) {
        this.favorite_number = favorite_number;
    }

    public String getFavorite_color() {
        return favorite_color;
    }

    public void setFavorite_color(String favorite_color) {
        this.favorite_color = favorite_color;
    }
}

package redis_asyn;

public class CC {

    private String msisdn;

    private String text;

    public String getMsisdn() {
        return msisdn;
    }

    public void setMsisdn(String msisdn) {
        this.msisdn = msisdn;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    @Override
    public String toString() {
        return "CC{" +
                "msisdn='" + msisdn + '\'' +
                ", text='" + text + '\'' +
                '}';
    }
}

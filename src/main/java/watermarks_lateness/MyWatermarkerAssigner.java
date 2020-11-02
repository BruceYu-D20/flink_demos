package watermarks_lateness;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import javax.annotation.Nullable;

public class MyWatermarkerAssigner implements AssignerWithPunctuatedWatermarks<Tuple2<Long, String>> {

    private final long maxOutOfOrderness = 3000; // 3 seconds
    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(Tuple2<Long, String> element, long previousElementTimestamp) {
        long eventTime = element.f0;
        currentMaxTimestamp = Math.max(eventTime, currentMaxTimestamp);
        return eventTime;
    }

    @Nullable
    @Override
    public Watermark checkAndGetNextWatermark(Tuple2<Long, String> lastElement, long extractedTimestamp) {

        System.out.println("extractedTimestamp: " + extractedTimestamp + " currentMaxTimestamp: " + currentMaxTimestamp);
        if(extractedTimestamp >= currentMaxTimestamp){
            Watermark watermark = new Watermark(extractedTimestamp - maxOutOfOrderness);
            return watermark;
        }else{
            return null;
        }
    }

}

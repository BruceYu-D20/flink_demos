package yux.broadcast.UserPurchaseBehavior.common;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;


public class MyPeriodWaterMark implements AssignerWithPeriodicWatermarks<Row>{

    private final long maxOutOfOrderness = 3000; // 10 seconds

    private long currentMaxTimestamp;

    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(Row element, long previousElementTimestamp) {

        long timestamp = Long.parseLong((String) element.getField(0));

        currentMaxTimestamp = Math.max(currentMaxTimestamp, timestamp);
        System.out.println("data event time: " + timestamp + ", watermark:" + getCurrentWatermark());
        return timestamp;
    }
}

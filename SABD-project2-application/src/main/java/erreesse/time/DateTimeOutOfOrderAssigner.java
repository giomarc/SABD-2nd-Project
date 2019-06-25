package erreesse.time;

import erreesse.pojo.CommentInfoPOJO;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

public class DateTimeOutOfOrderAssigner extends BoundedOutOfOrdernessTimestampExtractor<CommentInfoPOJO> {


    public DateTimeOutOfOrderAssigner() {
        super(Time.seconds(5));
    }

    @Override
    public long extractTimestamp(CommentInfoPOJO commentInfoPOJO) {
        return commentInfoPOJO.getCreateDate()*1000L;
    }
}
package erreesse.time;

import erreesse.pojo.CommentInfoPOJO;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class CreateDateTimeAssigner extends AscendingTimestampExtractor<CommentInfoPOJO> {

    public CreateDateTimeAssigner() {

    }

    @Override
    public long extractAscendingTimestamp(CommentInfoPOJO commentInfoPOJO) {
        return commentInfoPOJO.getCreateDate();
    }
}

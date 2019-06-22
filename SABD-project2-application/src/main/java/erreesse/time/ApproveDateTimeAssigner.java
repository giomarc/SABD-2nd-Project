package erreesse.time;

import erreesse.pojo.CommentInfoPOJO;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;

public class ApproveDateTimeAssigner extends AscendingTimestampExtractor<CommentInfoPOJO> {

    public ApproveDateTimeAssigner() {

    }

    @Override
    public long extractAscendingTimestamp(CommentInfoPOJO commentInfoPOJO) {
        return commentInfoPOJO.getCreateDate();
    }
}

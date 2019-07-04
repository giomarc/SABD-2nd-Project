package erreesse.pojo;

import erreesse.metrics.LatencyMarker;
import lombok.Getter;

public class LatencyCommentInfoPojo extends CommentInfoPOJO implements LatencyMarker {
    @Getter
    protected long startTime = 0L;
    @Getter
    protected long endTime = 0L;

    @Override
    public void setStartTime() {
        startTime = System.nanoTime();
    }

    @Override
    public void setStartTime(long st) {
        startTime = st;
    }

    @Override
    public void setEndTime() {
        endTime = System.nanoTime();
        if (startTime == 0L || startTime> endTime) {
            throw new IllegalStateException("setStartTime not called");
        }
    }

    @Override
    public void setEndTime(long et) {
        endTime = et;
    }

    @Override
    public long getElapsedTime() {
        if (startTime == 0L || endTime == 0L) {
            throw new IllegalStateException("Measurement not completed yet. Call setEndTime() before.");
        }
        return endTime-startTime;
    }

    public LatencyCommentInfoPojo(CommentInfoPOJO cip) {
        approvedDate= cip.approvedDate;
        articleID= cip.articleID;

        commentID= cip.commentID;
        commentType= cip.commentType;
        createDate= cip.createDate;
        depth= cip.depth;

        editorsSelection= cip.editorsSelection;
        inReplyTo= cip.inReplyTo;

        recommendations= cip.recommendations;
        userDisplayName= cip.userDisplayName;
        parentUserDisplayName= cip.parentUserDisplayName;
        userID= cip.userID;

        setStartTime();

    }
}

package erreesse.operators.filter;

import erreesse.pojo.CommentInfoPOJO;
import org.apache.flink.api.common.functions.FilterFunction;

public class CommentInfoPOJOValidator implements FilterFunction<CommentInfoPOJO> {
    @Override
    public boolean filter(CommentInfoPOJO cip) throws Exception {
        // blocks malformed records
        return cip!=null && cip.getCommentID() > 0 && cip.getUserID() > 0;
    }
}
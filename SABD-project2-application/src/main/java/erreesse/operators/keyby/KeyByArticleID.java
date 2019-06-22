package erreesse.operators.keyby;

import erreesse.pojo.CommentInfoPOJO;
import org.apache.flink.api.java.functions.KeySelector;

public class KeyByArticleID implements KeySelector<CommentInfoPOJO, String> {
    @Override
    public String getKey(CommentInfoPOJO commentInfoPOJO) throws Exception {
        return commentInfoPOJO.getArticleID();
    }
}

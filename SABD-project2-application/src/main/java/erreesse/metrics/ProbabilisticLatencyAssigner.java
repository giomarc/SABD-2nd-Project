package erreesse.metrics;

import erreesse.metrics.statistics.Distributions;
import erreesse.pojo.CommentInfoPOJO;
import erreesse.pojo.LatencyCommentInfoPojo;
import org.apache.flink.api.common.functions.MapFunction;

public class ProbabilisticLatencyAssigner implements MapFunction<CommentInfoPOJO, CommentInfoPOJO> {

    private double probThreshold = 0.4;
    @Override
    public CommentInfoPOJO map(CommentInfoPOJO commentInfoPOJO) throws Exception {

        double pExtracted = Distributions.getInstance().uniform(0, 1);
        CommentInfoPOJO result = commentInfoPOJO;

        if (pExtracted <= probThreshold) {
            result = new LatencyCommentInfoPojo(commentInfoPOJO);
        }
        return result;
    }
}

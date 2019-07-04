package erreesse.operators.map;

import erreesse.metrics.LatencyTuple1;
import erreesse.pojo.CommentInfoPOJO;
import erreesse.pojo.LatencyCommentInfoPojo;
import erreesse.pojo.PojoHelper;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TwoHourMapFunction implements MapFunction<CommentInfoPOJO, LatencyTuple1<Integer>> {
    @Override
    public LatencyTuple1<Integer> map(CommentInfoPOJO commentInfoPOJO) throws Exception {

        int fascia = PojoHelper.getFascia(commentInfoPOJO);
        LatencyTuple1<Integer> resultMap = new LatencyTuple1<>(fascia);


        if (commentInfoPOJO instanceof LatencyCommentInfoPojo) {
            LatencyCommentInfoPojo lcip = (LatencyCommentInfoPojo) commentInfoPOJO;
            resultMap.setStartTime(lcip.getStartTime());
        }

        return resultMap;
    }

}

package erreesse.operators.map;

import erreesse.pojo.CommentInfoPOJO;
import erreesse.pojo.PojoHelper;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TwoHourMapFunction implements MapFunction<CommentInfoPOJO, Integer> {
    @Override
    public Integer map(CommentInfoPOJO commentInfoPOJO) throws Exception {


        return PojoHelper.getFascia(commentInfoPOJO);
    }

}

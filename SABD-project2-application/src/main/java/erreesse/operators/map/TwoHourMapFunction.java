package erreesse.operators.map;

import erreesse.pojo.CommentInfoPOJO;
import org.apache.flink.api.common.functions.MapFunction;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class TwoHourMapFunction implements MapFunction<CommentInfoPOJO, Integer> {
    @Override
    public Integer map(CommentInfoPOJO commentInfoPOJO) throws Exception {
        return getTwoHourTime(commentInfoPOJO);
    }

    private Integer getTwoHourTime(CommentInfoPOJO cip) {
        long stdTimeStamp = cip.getCreateDate(); // numb of seconds since 1-1-1970
        LocalDateTime ldt = LocalDateTime.ofEpochSecond(stdTimeStamp,0, ZoneOffset.UTC);
        return getTwoHourKey(ldt.getHour());
    }

    private int getTwoHourKey(int hour) {
        // hour 15 -> 7
        int remind = hour / 2;
        return remind;
    }


}

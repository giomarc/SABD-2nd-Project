package erreesse.pojo;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class PojoHelper {

    public static int getFascia(CommentInfoPOJO cip) {
        long stdTimeStamp = cip.getCreateDate(); // numb of seconds since 1-1-1970
        LocalDateTime ldt = LocalDateTime.ofEpochSecond(stdTimeStamp,0, ZoneOffset.UTC);
        return getTwoHourKey(ldt.getHour());
    }

    private static int getTwoHourKey(int hour) {
        // hour 15 -> 7
        int remind = hour / 2;
        return remind;
    }
}

package erreesse.operators.windowassigner;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Collections;

public class MonthWindowAssigner extends TumblingEventTimeWindows {


    public MonthWindowAssigner() {
        super(Time.days(50).toMilliseconds(), 0L);
    }

    @Override
    public Collection<TimeWindow> assignWindows(Object element, long timestamp, WindowAssignerContext context) {
        LocalDateTime lastTS = LocalDateTime.ofEpochSecond(timestamp/1000L,0, ZoneOffset.UTC);

        long start = getLocalDateStartOfMonth(lastTS);
        long end = getLocalDateEndOfMonth(lastTS);

        // new TimeWindow(long start,long end)
        return Collections.singletonList(new TimeWindow(start, end));
    }

    private long getLocalDateStartOfMonth(LocalDateTime ldt) {
        // 18 Jan 2018
        // start -> 01 Jan 2018
        LocalDateTime startDT = LocalDateTime.of(ldt.getYear(),ldt.getMonthValue(),1,0,0);

        return  startDT.toEpochSecond(ZoneOffset.UTC)*1000L;

    }

    private long getLocalDateEndOfMonth(LocalDateTime ldt) {
        // 18 Jan 2018
        // end -> 31 Jan 2018

        boolean leapYear = ldt.toLocalDate().isLeapYear();
        int lastDayOfMonth = ldt.getMonth().length(leapYear);


        LocalDateTime endtDT = LocalDateTime.of(ldt.getYear(),ldt.getMonth(),lastDayOfMonth,23,59,59);

        return  endtDT.toEpochSecond(ZoneOffset.UTC)*1000L;
    }

    /*public static void main(String[] args) {

        MonthWindowAssigner assigner = new MonthWindowAssigner();

        long t1 = 1514767661;

        LocalDateTime lastTS = LocalDateTime.ofEpochSecond(t1,0, ZoneOffset.UTC);

        long start = assigner.getLocalDateStartOfMonth(lastTS);
        long end = assigner.getLocalDateEndOfMonth(lastTS);

        System.out.println("t1: "+t1);
        System.out.println("lastTS: "+lastTS.toString());

        System.out.println("start: "+start);
        System.out.println("end: "+end);

    }*/
}

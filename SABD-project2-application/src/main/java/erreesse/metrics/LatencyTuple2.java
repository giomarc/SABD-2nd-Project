package erreesse.metrics;

import lombok.Getter;

public class LatencyTuple2<T1,T2> extends scala.Tuple2<T1,T2> implements LatencyMarker {

    public LatencyTuple2(T1 _1, T2 _2) {
        super(_1,_2);
    }

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
}

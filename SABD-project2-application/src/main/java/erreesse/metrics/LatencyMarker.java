package erreesse.metrics;

public interface LatencyMarker {

    void setStartTime();
    void setStartTime(long startTime);

    void setEndTime();
    void setEndTime(long endTime);

    long getElapsedTime();
}

package erreesse.operators.aggregator;

import org.apache.flink.api.common.functions.AggregateFunction;

public class CommentCounterAggregator implements AggregateFunction<Integer,Long,Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(Integer integer, Long aLong) {
        return aLong + 1L;
    }

    @Override
    public Long getResult(Long aLong) {
        return aLong;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}

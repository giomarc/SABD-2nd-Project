package erreesse.operators.aggregator;

import erreesse.pojo.CommentInfoPOJO;
import org.apache.flink.api.common.functions.AggregateFunction;

/*
* IN, ACC, OUT
* */
public class ArticleCounterAggregator implements AggregateFunction<CommentInfoPOJO,Long,Long> {
    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(CommentInfoPOJO commentInfoPOJO, Long accumulator) {
        return accumulator+1L;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long aLong, Long acc1) {
        return aLong + acc1;
    }
}

package erreesse.operators.aggregator;

import erreesse.metrics.LatencyTuple1;
import erreesse.pojo.CommentInfoPOJO;
import erreesse.pojo.LatencyCommentInfoPojo;
import org.apache.flink.api.common.functions.AggregateFunction;

/*
* IN, ACC, OUT
* */
public class ArticleCounterAggregator implements AggregateFunction<CommentInfoPOJO, LatencyTuple1<Long>, LatencyTuple1<Long>> {

    @Override
    // create empty accumulator
    public LatencyTuple1<Long> createAccumulator() {
        return new LatencyTuple1<>(0L);
    }

    @Override
    // increment accumulator
    // for latency tracking: update the max timestamp viewed in window
    public LatencyTuple1<Long> add(CommentInfoPOJO commentInfoPOJO, LatencyTuple1<Long> accumulator) {
        long counter = accumulator._1 +1;
        LatencyTuple1 result = new LatencyTuple1<>(counter);

        if (commentInfoPOJO instanceof LatencyCommentInfoPojo) {
            LatencyCommentInfoPojo lcip = (LatencyCommentInfoPojo) commentInfoPOJO;
            long lastProcessingTime = Math.max(lcip.getStartTime(),accumulator.getStartTime());
            result.setStartTime(lastProcessingTime);
        }

        return result;
    }

    @Override
    // return the final accumulator, i.e. counter
    public LatencyTuple1<Long> getResult(LatencyTuple1<Long> accumulator) {
        return accumulator;
    }

    @Override
    // merge two different accumulator
    // for latency tracking: update the timestamp with max
    public LatencyTuple1<Long> merge(LatencyTuple1<Long> acc1, LatencyTuple1<Long> acc2) {
        long counter = acc1._1 + acc2._1;
        long lastTimeStamp = Math.max(acc1.getStartTime(),acc2.getStartTime());

        LatencyTuple1<Long> mergeAcc = new LatencyTuple1<>(counter);
        mergeAcc.setStartTime(lastTimeStamp);
        return mergeAcc;
    }
}

package erreesse.operators.aggregator;

import erreesse.pojo.CommentInfoPOJO;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple;
import scala.Tuple2;

/*
* IN, ACC, OUT
* */
public class ArticleCounterAggregator implements AggregateFunction<CommentInfoPOJO,Tuple2<Long, Long>, Tuple2<Long, Long>> {

    @Override
    public Tuple2<Long,Long> createAccumulator() {
        return new Tuple2<>(0L,0L);
    }

    @Override
    public Tuple2<Long, Long> add(CommentInfoPOJO commentInfoPOJO, Tuple2<Long, Long> accumulator) {
        long counter = accumulator._1;
        counter +=1;

        long lastProcessingTime = commentInfoPOJO.processingTime;
        long lastAccumProcessingTime = accumulator._2;
        if (lastAccumProcessingTime<lastProcessingTime) lastAccumProcessingTime = lastProcessingTime;

        return new Tuple2<>(counter,lastAccumProcessingTime);
    }

    @Override
    public Tuple2<Long, Long> getResult(Tuple2<Long, Long> accumulator) {
        return accumulator;
    }

    @Override
    public Tuple2<Long, Long> merge(Tuple2<Long, Long> acc1, Tuple2<Long, Long> acc2) {
        long counter = acc1._1 + acc2._1;
        long lastTimeStamp = Math.max(acc1._2,acc2._2);

        return new Tuple2<Long, Long>(counter,lastTimeStamp);
    }
}

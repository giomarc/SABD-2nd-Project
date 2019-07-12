package erreesse.operators.aggregator;

import erreesse.metrics.LatencyTuple1;
import org.apache.flink.api.common.functions.AggregateFunction;

// IN -> LatencyTuple1<Integer>
// ACC -> FasciaArrayAccumulator
// OUT -> FasciaArrayAccumulator
public class FasciaAggregator implements AggregateFunction<LatencyTuple1<Integer>, LatencyTuple1<FasciaArrayAccumulator>, LatencyTuple1<FasciaArrayAccumulator>> {
    @Override
    // create empty accumulator
    public LatencyTuple1<FasciaArrayAccumulator> createAccumulator() {
        FasciaArrayAccumulator accum = new FasciaArrayAccumulator();
        return new LatencyTuple1<>(accum);
    }

    @Override
    // increment accumulator, hit in correct index
    // for latency tracking: update the max timestamp viewed in window
    public LatencyTuple1<FasciaArrayAccumulator> add(LatencyTuple1<Integer> toAdd, LatencyTuple1<FasciaArrayAccumulator> fasciaArrayAccumulator) {
        fasciaArrayAccumulator._1.hit(toAdd._1.intValue());
        long timeStamp = Math.max(fasciaArrayAccumulator.getStartTime(),toAdd.getStartTime());
        fasciaArrayAccumulator.setStartTime(timeStamp);

        return fasciaArrayAccumulator;
    }

    @Override
    // return the final accumulator, i.e. fasciaaccumulator (12 int array)
    public LatencyTuple1<FasciaArrayAccumulator> getResult(LatencyTuple1<FasciaArrayAccumulator> fasciaArrayAccumulator) {
        return fasciaArrayAccumulator;
    }

    @Override
    // merge two different accumulator
    // for latency tracking: update the timestamp with max
    public LatencyTuple1<FasciaArrayAccumulator> merge(LatencyTuple1<FasciaArrayAccumulator> latAcc1, LatencyTuple1<FasciaArrayAccumulator> latAcc2) {

        FasciaArrayAccumulator acc1 = latAcc1._1;
        FasciaArrayAccumulator acc2 = latAcc2._1;

        FasciaArrayAccumulator accMerged = acc1.mergeWith(acc2);
        long lastTimeStamp = Math.max(latAcc1.getStartTime(),latAcc2.getStartTime());

        LatencyTuple1<FasciaArrayAccumulator> latAccMerger = new LatencyTuple1<FasciaArrayAccumulator>(accMerged);
        latAccMerger.setStartTime(lastTimeStamp);

        return latAccMerger;
    }
}

package erreesse.operators.aggregator;

import org.apache.flink.api.common.functions.AggregateFunction;

// IN -> integer
// ACC -> FasciaArrayAccumulator
// OUT -> FasciaArrayAccumulator
public class FasciaAggregator implements AggregateFunction<Integer, FasciaArrayAccumulator, FasciaArrayAccumulator> {
    @Override
    public FasciaArrayAccumulator createAccumulator() {
        return new FasciaArrayAccumulator();
    }

    @Override
    public FasciaArrayAccumulator add(Integer integer, FasciaArrayAccumulator fasciaArrayAccumulator) {
        fasciaArrayAccumulator.hit(integer.intValue());
        return fasciaArrayAccumulator;
    }

    @Override
    public FasciaArrayAccumulator getResult(FasciaArrayAccumulator fasciaArrayAccumulator) {
        return fasciaArrayAccumulator;
    }

    @Override
    public FasciaArrayAccumulator merge(FasciaArrayAccumulator acc1, FasciaArrayAccumulator acc2) {
        return acc1.mergeWith(acc2);
    }
}

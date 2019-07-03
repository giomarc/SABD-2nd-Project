package erreesse.metrics;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.dropwizard.metrics.DropwizardMeterWrapper;
import org.apache.flink.metrics.Meter;
import org.apache.flink.streaming.api.functions.windowing.RichAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;

public class ComputeThroughputFunction extends RichAllWindowFunction<Object,Object,Window> {

    private transient Meter meter;
    private String operatorName;

    public ComputeThroughputFunction(String aOperatorName) {
        operatorName = aOperatorName;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        com.codahale.metrics.Meter dropwizardMeter = new com.codahale.metrics.Meter();
        meter = getRuntimeContext()
                .getMetricGroup()
                .meter("ErreEsseMeter: "+operatorName, new DropwizardMeterWrapper(dropwizardMeter));
    }

    @Override
    public void apply(Window window, Iterable<Object> iterable, Collector<Object> collector) throws Exception {
        meter.markEvent();
    }
}

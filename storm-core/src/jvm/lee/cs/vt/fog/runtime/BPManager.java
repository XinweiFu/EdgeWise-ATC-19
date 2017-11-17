package lee.cs.vt.fog.runtime;

import org.apache.storm.spout.ISpout;

import java.util.List;

public class BPManager {
    private final List<ISpout> spouts;

    public BPManager(List<ISpout> spouts) {
        this.spouts = spouts;
    }

    private boolean last_backpressure_on = false;

    public void backpressure_on() {
        if (!last_backpressure_on) {
            last_backpressure_on = true;

            for (ISpout spout : spouts)
                spout.ack(null);
        }
    }

    public void backpressure_off() {
        if (last_backpressure_on) {
            last_backpressure_on = false;

            for (ISpout spout : spouts)
                spout.fail(null);
        }
    }
}

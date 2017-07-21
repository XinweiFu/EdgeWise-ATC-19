package lee.cs.vt.fog.runtime;

import org.apache.storm.utils.DisruptorQueue;

public class BoltRuntimeUnit extends RuntimeUnit{
    private final DisruptorQueue queue;

    public BoltRuntimeUnit(DisruptorQueue queue, ExecutorCallback callback) {
        super(callback);
        assert(callback.getType() == ExecutorCallback.ExecutorType.bolt);

        this.queue = queue;
    }

    public long getNumInQ() {
        DisruptorQueue.QueueMetrics m = queue.getMetrics();
        return m.population();
    }
}

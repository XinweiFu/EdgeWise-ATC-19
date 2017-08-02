package lee.cs.vt.fog.runtime;

import org.apache.storm.utils.DisruptorQueue;

public class BoltRuntimeUnit extends RuntimeUnit{
    private final BoltReceiveDisruptorQueue queue;

    public BoltRuntimeUnit(BoltReceiveDisruptorQueue queue, ExecutorCallback callback) {
        super(callback);
        assert(callback.getType() == ExecutorCallback.ExecutorType.bolt);

        this.queue = queue;
    }

    public long getNumInQ() {
        DisruptorQueue.QueueMetrics m = queue.getMetrics();
        return m.population();
    }

    public void setIsRunning() {
        queue.setIsRunning();
    }

    public void resetIsRunning() {
        queue.resetIsRunning();
    }
}

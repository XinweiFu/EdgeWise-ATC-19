package lee.cs.vt.fog.runtime.unit;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import org.apache.storm.utils.DisruptorQueue;

public class BoltRuntimeUnit extends RuntimeUnit{
    private final String componentId;
    private final BoltReceiveDisruptorQueue queue;

    private int waitedCount = 0;

    public BoltRuntimeUnit(String componentId,
                           BoltReceiveDisruptorQueue queue,
                           ExecutorCallback callback) {
        super(callback);
        assert(callback.getType() == ExecutorCallback.ExecutorType.bolt);

        this.componentId = componentId;
        this.queue = queue;
    }

    public String getComponentId() {
        return componentId;
    }

    public String getName() {
        return componentId + "," + queue.getName();
    }

    @Override
    public String toString() {
        return getName();
    }

    public Long getWaitedTime() {
        return queue.getWaitedTime();
    }

    public void setReadyTime() {
        queue.setReadyTime();
    }

    public void resetReadyTime () {
        queue.resetReadyTime();
    }

    public int getWaitedCount() {
        return waitedCount;
    }

    public void incWaitedCount() {
        waitedCount++;
    }

    public void resetWaitedCount() {
        waitedCount = 0;
    }

    public long getNumInQ() {
        DisruptorQueue.QueueMetrics m = queue.getMetrics();
        return m.population();
    }

    public void print() {
        System.out.println("BoltRuntimeUnit componentId:" + componentId + " queue:" + queue.getName());
    }
}

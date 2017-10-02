package lee.cs.vt.fog.runtime.unit;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import org.apache.storm.utils.DisruptorQueue;

public class BoltRuntimeUnit extends RuntimeUnit{
    private final String componentId;
    private final BoltReceiveDisruptorQueue queue;

    private Long lastCompletedTime = new Long(0);

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
        Long curr = System.nanoTime();
        if (lastCompletedTime == 0) {
            return lastCompletedTime;
        } else {
            Long waitedTime = curr - lastCompletedTime;
            assert(waitedTime >= 0);
            return waitedTime;
        }
    }

    public void setCompletedTime() {
        lastCompletedTime = System.nanoTime();
    }

    public long getNumInQ() {
        DisruptorQueue.QueueMetrics m = queue.getMetrics();
        return m.population();
    }

    public void print() {
        System.out.println("BoltRuntimeUnit componentId:" + componentId + " queue:" + queue.getName());
    }
}

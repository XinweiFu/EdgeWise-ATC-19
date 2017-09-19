package lee.cs.vt.fog.runtime.unit;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import org.apache.storm.utils.DisruptorQueue;

public class BoltRuntimeUnit extends RuntimeUnit{
    private final String componentId;
    private final BoltReceiveDisruptorQueue queue;

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

    public long getNumInQ() {
        DisruptorQueue.QueueMetrics m = queue.getMetrics();
        return m.population();
    }

    public void print() {
        System.out.println("BoltRuntimeUnit componentId:" + componentId + " queue:" + queue.getName());
    }
}

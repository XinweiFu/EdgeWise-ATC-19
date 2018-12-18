package lee.cs.vt.fog.runtime.unit;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.policy.RuntimePolicy;
import org.apache.storm.utils.DisruptorQueue;

public class BoltRuntimeUnit extends RuntimeUnit{
    private final String componentId;
    protected final BoltReceiveDisruptorQueue queue;
    private RuntimePolicy policy = null;
    private boolean isRunning = false;

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

    public BoltReceiveDisruptorQueue getQueue() {
        return queue;
    }

    public void setIsRunning(boolean isRunning) {
        this.isRunning = isRunning;
    }

    public boolean isRunning() {
        return isRunning;
    }

    @Override
    public String toString() {
        return getName();
    }

    public long getNumInQ() {
        DisruptorQueue.QueueMetrics m = queue.getMetrics();
        return m.population();
    }

    public void setPolicy(RuntimePolicy policy) {
        this.policy = policy;
    }

    public void updateEmptyQueue() {
        policy.updateEmptyQueue(this);
    }

    public String printAverageWaitTime() {
        long totalTupleConsumed = queue.getTotalTupleConsumed();
        long totalWaitTime = queue.getTotalWaitTime();
        double averageWaitTime = totalWaitTime / (double) totalTupleConsumed;

        String ret = "";
        ret += componentId + ",";
        ret += queue.getName() + ",";
        ret += totalTupleConsumed + ",";
        ret += totalWaitTime + ",";
        ret += averageWaitTime + "\n";
        return ret;
    }

    public String printTotalEmptyTime() {
        long totalTupleConsumed = queue.getTotalTupleConsumed();
        long totalEmptyTime = queue.getTotalEmptyTime();
        double averageEmptyTime = totalEmptyTime / (double) totalTupleConsumed;

        String ret = "";
        ret += componentId + ",";
        ret += queue.getName() + ",";
        ret += totalTupleConsumed + ",";
        ret += totalEmptyTime + ",";
        ret += averageEmptyTime + "\n";
        return ret;
    }

    public void print() {
        System.out.println("BoltRuntimeUnit componentId:" + componentId + " queue:" + queue.getName());
    }
}

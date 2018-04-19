package lee.cs.vt.fog.runtime.unit;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.misc.WeightManager;

public class WeightBoltRuntimeUnit extends BoltRuntimeUnit {
    private final int weight;
    private final WeightManager weightManager;
    private boolean isRunning = false;

    public WeightBoltRuntimeUnit(String componentId,
                                 BoltReceiveDisruptorQueue queue,
                                 ExecutorCallback callback,
                                 int weight,
                                 WeightManager weightManager) {
        super(componentId, queue, callback);
        this.weight = weight;
        this.weightManager = weightManager;
    }

    public int getWeight() {
        return weight;
    }

    public void setIsRunning(boolean isRunning) {
        this.isRunning = isRunning;
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void updateEmptyQueue() {
        weightManager.updateEmptyQueue(this);
    }

    @Override
    public void print() {
        super.print();
        System.out.println("WeightBoltRuntimeUnit weight = " +  weight);
    }
}

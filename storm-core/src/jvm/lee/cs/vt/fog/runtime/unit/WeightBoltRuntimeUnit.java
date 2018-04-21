package lee.cs.vt.fog.runtime.unit;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.misc.WeightManager;
import lee.cs.vt.fog.runtime.policy.RuntimePolicy;

public class WeightBoltRuntimeUnit extends BoltRuntimeUnit {
    private final int weight;

    public WeightBoltRuntimeUnit(String componentId,
                                 BoltReceiveDisruptorQueue queue,
                                 ExecutorCallback callback,
                                 int weight) {
        super(componentId, queue, callback);
        this.weight = weight;
    }

    public int getWeight() {
        return weight;
    }

    @Override
    public void print() {
        super.print();
        System.out.println("WeightBoltRuntimeUnit weight = " +  weight);
    }
}

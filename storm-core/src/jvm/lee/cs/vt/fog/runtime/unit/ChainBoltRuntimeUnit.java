package lee.cs.vt.fog.runtime.unit;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;

public class ChainBoltRuntimeUnit extends BoltRuntimeUnit {
    private final int priority;

    public ChainBoltRuntimeUnit(String componentId,
                                BoltReceiveDisruptorQueue queue,
                                ExecutorCallback callback,
                                int priority) {
        super(componentId, queue, callback);
        this.priority = priority;
    }

    public int getPrio() {
        return priority;
    }

    public long getChainTimestamp() {
        return queue.getChainTimestamp();
    }

    @Override
    public void print() {
        super.print();
        System.out.println("ChainBoltRuntimeUnit priority = " +  priority);
    }
}

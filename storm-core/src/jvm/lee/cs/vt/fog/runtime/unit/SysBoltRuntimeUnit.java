package lee.cs.vt.fog.runtime.unit;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;

public class SysBoltRuntimeUnit extends RuntimeUnit {
    private final String componentId;
    private final BoltReceiveDisruptorQueue queue;

    public SysBoltRuntimeUnit(String componentId,
                              BoltReceiveDisruptorQueue queue,
                              ExecutorCallback callback) {
        super(callback);
        assert(callback.getType() == ExecutorCallback.ExecutorType.bolt);

        this.componentId = componentId;
        this.queue = queue;
    }

    public void print() {
        System.out.println("SysBoltRuntimeUnit componentId:" + componentId +
                            " queue:" + queue.getName());
    }
}
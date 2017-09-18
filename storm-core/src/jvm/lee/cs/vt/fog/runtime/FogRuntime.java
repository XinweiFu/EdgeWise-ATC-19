package lee.cs.vt.fog.runtime;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.policy.*;
import lee.cs.vt.fog.runtime.thread.BoltThread;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnitContainer;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnitGroup;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FogRuntime {

    public static final Lock LOCK = new ReentrantLock();;
    public static final Condition CONDITION = LOCK.newCondition();

    private final BoltRuntimeUnitContainer container;

    private final Set<BoltThread> boltThreads;
    private final RuntimePolicy policy;

    public FogRuntime (List<ExecutorCallback.CallbackProvider> list,
                       Map<Object, BoltReceiveDisruptorQueue> map,
                       int numThreadPoll,
                       String policyString) {
        Map<String, Set<BoltRuntimeUnit>> unitsMap = new HashMap<String,Set<BoltRuntimeUnit>>();

        for (ExecutorCallback.CallbackProvider provider : list) {
            ExecutorCallback callback = provider.getCallback();
            if (callback == null)
                continue;
            Object executorId = callback.getExecutorId();
            BoltReceiveDisruptorQueue queue = map.get(executorId);
            assert(queue != null);

            String componentId = callback.getComponentId();

            ExecutorCallback.ExecutorType type = callback.getType();
            switch (type) {
                case bolt:
                    if (!unitsMap.containsKey(componentId))
                        unitsMap.put(componentId, new HashSet<BoltRuntimeUnit>());
                    unitsMap.get(componentId).add(new BoltRuntimeUnit(componentId, queue, callback));
                    break;
                default:
                    // Never comes here
                    assert(false);
            }
        }

        container = new BoltRuntimeUnitContainer(unitsMap.values(), numThreadPoll);

        // spoutThread = new SpoutThread(spouts);
        if (policyString == null)
            policy = new SimpleSignalRuntimePolicy(container.getGroups());
        else {
            switch (policyString) {
                case "signal":
                    policy = new SimpleSignalRuntimePolicy(container.getGroups());
                    break;
                default:
                    policy = new SimpleSignalRuntimePolicy(container.getGroups());
            }
        }


        boltThreads = new HashSet<BoltThread>();
        for (int i = 0; i < numThreadPoll; i ++) {
            BoltThread boltThread = new BoltThread(policy);
            boltThreads.add(boltThread);
        }

        print();
    }

    public void start() {
        for (BoltThread boltThread : boltThreads) {
            boltThread.start();
        }
    }

    public void stop() throws InterruptedException {
        for (BoltThread boltThread : boltThreads) {
            boltThread.stopAndWait();
        }
    }

    private void print() {
        System.out.println("Fog Runtime Print begins:");

        for (BoltRuntimeUnitGroup group : container.getGroups()) {
            group.print();
        }

        System.out.println("Fog Runtime Print ends");
    }
}

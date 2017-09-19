package lee.cs.vt.fog.runtime;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.policy.*;
import lee.cs.vt.fog.runtime.thread.BoltThread;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnitGroup;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FogRuntime {

    public static final Lock LOCK = new ReentrantLock();;
    public static final Condition CONDITION = LOCK.newCondition();

    private final Set<BoltThread> boltThreads;
    private final RuntimePolicy policy;

    public FogRuntime (List<ExecutorCallback.CallbackProvider> list,
                       Map<Object, BoltReceiveDisruptorQueue> map,
                       int numThreadPoll,
                       String policyString) {
        if (policyString == null)
            policy = new SimpleSignalRuntimePolicy(list, map);
        else {
            switch (policyString) {
                case "signal-simple":
                    policy = new SimpleSignalRuntimePolicy(list, map);
                    break;
                case "signal-group":
                    policy = new GroupSignalRuntimePolicy(list, map);
                    break;
                default:
                    policy = new SimpleSignalRuntimePolicy(list, map);
                    break;
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
        policy.print();
        System.out.println("Fog Runtime Print ends");
    }
}

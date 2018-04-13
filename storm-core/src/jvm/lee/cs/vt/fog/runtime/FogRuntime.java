package lee.cs.vt.fog.runtime;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.policy.*;
import lee.cs.vt.fog.runtime.thread.BoltThread;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FogRuntime {

    public static final Lock LOCK = new ReentrantLock();;
    public static final Condition CONDITION = LOCK.newCondition();

    public static boolean getWaitTime = false;
    public static boolean getEmptyTime = false;

    private final Set<BoltThread> boltThreads;
    private final RuntimePolicy policy;

    private final String storm_id;

    public FogRuntime (List<ExecutorCallback.CallbackProvider> list,
                       Map<Object, BoltReceiveDisruptorQueue> map,
                       String storm_id,
                       Map storm_conf) {
        final int numThreadPoll = 4;
        this.storm_id = storm_id;

        Object getWaitTime = storm_conf.get("get_wait_time");
        if (getWaitTime != null) {
            FogRuntime.getWaitTime = (boolean) getWaitTime;
        }

        Object getEmptyTime = storm_conf.get("get_empty_time");
        if (getEmptyTime != null) {
            FogRuntime.getEmptyTime = (boolean) getEmptyTime;
        }

        String policyString = (String) storm_conf.get("policy");

        if (policyString == null)
            policy = new EDADynamicPolicy(list, map);
        else {
            switch (policyString) {
                case "eda-dynamic":
                    policy = new EDADynamicPolicy(list, map);
                    break;
                default:
                    policy = new EDADynamicPolicy(list, map);
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
        System.out.println("Fog Runtime Stopping:");

        if (getWaitTime) {
            System.out.print(policy.printAverageWaitTime());
        }

        if (getEmptyTime) {
            System.out.print(policy.printTotalEmptyTime());
        }

        for (BoltThread boltThread : boltThreads) {
            boltThread.stopAndWait();
        }

        System.out.println("Fog Runtime Stopped");
    }

    private void print() {
        System.out.println("Fog Runtime Print begins:");
        policy.print();
        System.out.println("Fog Runtime Print ends");
    }
}

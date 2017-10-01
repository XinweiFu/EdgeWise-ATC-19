package lee.cs.vt.fog.runtime;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.policy.*;
import lee.cs.vt.fog.runtime.thread.BoltThread;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FogRuntime {

    public static final Lock LOCK = new ReentrantLock();;
    public static final Condition CONDITION = LOCK.newCondition();

    private final Set<BoltThread> boltThreads;
    private final RuntimePolicy policy;

    private final boolean debug;
    private final String debug_path;
    private final List<String> debug_info = new CopyOnWriteArrayList<String>();

    private final String storm_id;

    public FogRuntime (List<ExecutorCallback.CallbackProvider> list,
                       Map<Object, BoltReceiveDisruptorQueue> map,
                       String storm_id,
                       Map storm_conf) {
        final int numThreadPoll = 4;
        this.storm_id = storm_id;

        String debug_str = (String) storm_conf.get("fog-runtime-debug");
        String policyString = (String) storm_conf.get("policy");
        debug_path = (String) storm_conf.get("debug-path");

        if (debug_str == null)
            debug = false;
        else {
            switch (debug_str) {
                case "true":
                    debug = true;
                    break;
                case "false":
                    debug = false;
                    break;
                default:
                    debug = false;
                    break;
            }
        }


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
            BoltThread boltThread = new BoltThread(policy, debug, debug_info);
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

        if (debug) {
            System.out.println("Fog Runtime Debug:");

            Map<String, Long> totalTurnMap = new HashMap<String, Long>();
            Map<String, Long> totalConsumeTimeMap = new HashMap<String, Long>();
            Map<String, Set<Long>> totalWaitTimeMap = new HashMap<String, Set<Long>>();

            for (BoltThread boltThread : boltThreads) {

                Map<BoltRuntimeUnit, Long> turnMap = boltThread.getTurnMap();

                for (BoltRuntimeUnit unit : turnMap.keySet()) {
                    String name = unit.getName();
                    if (!totalTurnMap.containsKey(name))
                        totalTurnMap.put(name, turnMap.get(unit));
                    else
                        totalTurnMap.put(name, totalTurnMap.get(name) + turnMap.get(unit));
                }

                Map<BoltRuntimeUnit, Long> consumeTimeMap = boltThread.getConsumeTimeMap();

                for (BoltRuntimeUnit unit : consumeTimeMap.keySet()) {
                    String name = unit.getName();
                    if (!totalConsumeTimeMap.containsKey(name))
                        totalConsumeTimeMap.put(name, consumeTimeMap.get(unit));
                    else
                        totalConsumeTimeMap.put(name, totalConsumeTimeMap.get(name) + consumeTimeMap.get(unit));
                }

                Map<BoltRuntimeUnit, Set<Long>> waitTimeMap = boltThread.getWaitTimeMap();

                for (BoltRuntimeUnit unit : waitTimeMap.keySet()) {
                    String name = unit.getName();
                    if (!totalWaitTimeMap.containsKey(name))
                        totalWaitTimeMap.put(name, waitTimeMap.get(unit));
                    else
                        totalWaitTimeMap.get(name).addAll(waitTimeMap.get(unit));
                }
            }

            String fileName = "debug_info_" + storm_id;
            try {
                FileOutputStream f = new FileOutputStream(new File(debug_path + "/" + fileName));
                ObjectOutputStream o = new ObjectOutputStream(f);

                o.writeObject(this.debug_info);
                o.writeObject(totalTurnMap);
                o.writeObject(totalConsumeTimeMap);
                o.writeObject(totalWaitTimeMap);

                o.close();
                f.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
            
            System.out.println("Fog Runtime Debug finished.");
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

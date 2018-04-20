package lee.cs.vt.fog.runtime;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.policy.*;
import lee.cs.vt.fog.runtime.thread.BoltThread;
import lee.cs.vt.fog.runtime.thread.SysBoltThread;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;
import lee.cs.vt.fog.runtime.unit.SysBoltRuntimeUnit;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class FogRuntime {

    public static final Lock LOCK = new ReentrantLock();;
    public static final Condition CONDITION = LOCK.newCondition();

    public static boolean getWaitTime = false;
    public static boolean getEmptyTime = false;
    private String infoPath = null;
    public static boolean getQueueTime = false;

    private final Set<BoltThread> boltThreads;
    private final Set<SysBoltThread> sysBoltThreads;
    private final RuntimePolicy policy;

    private final String storm_id;
    private final String topoName;

    public FogRuntime (List<ExecutorCallback.CallbackProvider> list,
                       Map<Object, BoltReceiveDisruptorQueue> map,
                       String storm_id,
                       Map storm_conf) {
        final int numThreadPool = 4;
        this.storm_id = storm_id;

        String topoName = storm_id.substring(0, storm_id.lastIndexOf('-'));
        topoName = topoName.substring(0, topoName.lastIndexOf('-'));
        this.topoName = topoName;

        Object getWaitTime = storm_conf.get("get_wait_time");
        if (getWaitTime != null) {
            FogRuntime.getWaitTime = (boolean) getWaitTime;
        }

        Object getEmptyTime = storm_conf.get("get_empty_time");
        if (getEmptyTime != null) {
            FogRuntime.getEmptyTime = (boolean) getEmptyTime;
        }

        if (FogRuntime.getWaitTime ||
                FogRuntime.getEmptyTime) {
            infoPath = (String) storm_conf.get("info_path");
            assert(infoPath != null);
        }

        Object getQueueTime = storm_conf.get("get_queue_time");
        if (getQueueTime != null) {
            FogRuntime.getQueueTime = (boolean) getQueueTime;
        }

        sysBoltThreads = new HashSet<SysBoltThread>();
        Set<BoltRuntimeUnit> bolts = new HashSet<BoltRuntimeUnit>();
        for (ExecutorCallback.CallbackProvider e : list) {
            ExecutorCallback callback = e.getCallback();
            if (callback == null)
                continue;

            Object executorId = callback.getExecutorId();
            BoltReceiveDisruptorQueue queue = map.get(executorId);

            String componentId = callback.getComponentId();

            if (queue.isBolt()) {
                bolts.add(new BoltRuntimeUnit(componentId, queue, callback));
            } else {
                SysBoltRuntimeUnit unit = new SysBoltRuntimeUnit(componentId, queue, callback);
                sysBoltThreads.add(new SysBoltThread(unit));
            }

        }

        String policyString = (String) storm_conf.get("policy");
        if (policyString == null)
            policy = new EDADynamicPolicy(bolts);
        else {
            switch (policyString) {
                case "eda-dynamic":
                    policy = new EDADynamicPolicy(bolts);
                    break;
                case "eda-static":
                    policy = new EDAStaticPolicy(bolts, storm_conf);
                    break;
                case "eda-random":
                    policy = new EDARandomPolicy(bolts);
                    break;
                default:
                    policy = new EDADynamicPolicy(bolts);
                    break;
            }
        }


        boltThreads = new HashSet<BoltThread>();
        for (int i = 0; i < numThreadPool; i ++) {
            BoltThread boltThread = new BoltThread(policy);
            boltThreads.add(boltThread);
        }

        print();
    }

    public void start() {
        System.out.println("Fog Runtime Start begins:");
        for (SysBoltThread sysBoltThread : sysBoltThreads) {
            sysBoltThread.start();
        }
        for (BoltThread boltThread : boltThreads) {
            boltThread.start();
        }
        System.out.println("Fog Runtime Start ends.");
    }

    public void stop() throws InterruptedException {
        System.out.println("Fog Runtime Stopping:");

        if (getWaitTime) {
            printToInfo("wait_info", policy.printAverageWaitTime());
        }

        if (getEmptyTime) {
            printToInfo("empty_info", policy.printTotalEmptyTime());
        }

        for (SysBoltThread sysBoltThread : sysBoltThreads) {
            sysBoltThread.stopAndWait();
        }
        for (BoltThread boltThread : boltThreads) {
            boltThread.stopAndWait();
        }

        System.out.println("Fog Runtime Stopped");
    }

    private void printToInfo(String tail, String s) {
        String fileName = topoName + "_" + tail;
        PrintWriter pw = null;
        try   {
            File file = new File(infoPath + "/" + fileName);
            FileWriter fw = new FileWriter(file, false);
            pw = new PrintWriter(fw);
            pw.println(s);
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        } finally {
            if (pw != null) {
                pw.close();
            }
        }
    }

    private void print() {
        System.out.println("Fog Runtime Print begins:");
        for (SysBoltThread sysBoltThread : sysBoltThreads) {
            sysBoltThread.print();
        }
        policy.print();
        System.out.println("Fog Runtime Print ends");
    }
}

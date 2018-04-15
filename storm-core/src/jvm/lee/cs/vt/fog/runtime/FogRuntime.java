package lee.cs.vt.fog.runtime;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.policy.*;
import lee.cs.vt.fog.runtime.thread.BoltThread;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
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
    private String infoPath = null;

    private final Set<BoltThread> boltThreads;
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
        for (int i = 0; i < numThreadPool; i ++) {
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
            printToInfo("wait_info", policy.printAverageWaitTime());
        }

        if (getEmptyTime) {
            printToInfo("empty_info", policy.printTotalEmptyTime());
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
        policy.print();
        System.out.println("Fog Runtime Print ends");
    }
}

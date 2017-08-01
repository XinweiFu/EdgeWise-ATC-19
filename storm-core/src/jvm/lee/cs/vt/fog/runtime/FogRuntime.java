package lee.cs.vt.fog.runtime;

import org.apache.storm.utils.DisruptorQueue;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FogRuntime {

    // private final Set<ExecutorCallback> spouts;
    private final Set<BoltRuntimeUnit> bolts;

    // private final SpoutThread spoutThread;
    private final Set<BoltThread> boltThreads;
    private final RuntimePolicy policy;

    public FogRuntime (List<ExecutorCallback.CallbackProvider> list,
                       Map<Object, DisruptorQueue> map,
                       int numThreadPoll,
                       String policyString) {
        // spouts = new HashSet<ExecutorCallback>();
        bolts = new HashSet<BoltRuntimeUnit>();

        for (ExecutorCallback.CallbackProvider provider : list) {
            ExecutorCallback callback = provider.getCallback();
            if (callback == null)
                continue;
            Object executorId = callback.getExecutorId();
            DisruptorQueue queue = map.get(executorId);
            assert(queue != null);

            ExecutorCallback.ExecutorType type = callback.getType();
            switch (type) {
                // case spout:
                //    spouts.add(callback);
                //    break;
                case bolt:
                    bolts.add(new BoltRuntimeUnit(queue, callback));
                    break;
                default:
                    // Never comes here
                    assert(false);
            }
        }

        // spoutThread = new SpoutThread(spouts);
        if (policyString == null)
            policy = new SimpleRuntimePolicy(bolts);
        else {
            switch (policyString) {
                case "simple":
                    policy = new SimpleRuntimePolicy(bolts);
                    break;
                case "ranking":
                    policy = new RankingRuntimePolicy(bolts, numThreadPoll);
                    break;
                case "random":
                    policy = new RandomRuntimePolicy(bolts);
                    break;
                default:
                    policy = new SimpleRuntimePolicy(bolts);
            }
        }


        boltThreads = new HashSet<BoltThread>();
        for (int i = 0; i < numThreadPoll; i ++) {
            BoltThread boltThread = new BoltThread(policy);
            boltThreads.add(boltThread);
        }

        // test();
    }

    public void start() {
        // spoutThread.start();

        for (BoltThread boltThread : boltThreads) {
            boltThread.start();
        }
    }

    public void stop() throws InterruptedException {
        // spoutThread.stopAndWait();

        for (BoltThread boltThread : boltThreads) {
            boltThread.stopAndWait();
        }
    }

    private void test() {
        System.out.println("Fog Runtime Test begins:");

        /*
        System.out.println("Spouts:");
        for (ExecutorCallback spout : spouts) {
            System.out.println(spout.getExecutorId());
            System.out.println(spout.getType());
            spout.run();
        }
        */

        System.out.println("Bolts:");
        for (BoltRuntimeUnit unit : bolts) {
            ExecutorCallback bolt = unit.getCallback();
            System.out.println(bolt.getExecutorId());
            System.out.println(bolt.getType());
            bolt.run();
        }

        System.out.println("Fog Runtime Test ends");
    }
}

package lee.cs.vt.fog.runtime.policy;

import lee.cs.vt.fog.runtime.FogRuntime;
import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class FairSignalRuntimePolicy implements RuntimePolicy {

    private final long threshold;

    private final Set<BoltRuntimeUnit> bolts;
    private final Set<BoltRuntimeUnit> availableBolts;

    private final Lock lock = FogRuntime.LOCK;
    private final Condition condition = FogRuntime.CONDITION;

    public FairSignalRuntimePolicy(List<ExecutorCallback.CallbackProvider> list,
                                   Map<Object, BoltReceiveDisruptorQueue> map,
                                   long threshold) {
        this.bolts = new HashSet<BoltRuntimeUnit>();

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
                    this.bolts.add(new BoltRuntimeUnit(componentId, queue, callback));
                    break;
                default:
                    // Never comes here
                    assert(false);
            }
        }

        this.availableBolts = new HashSet<BoltRuntimeUnit>(bolts);

        this.threshold = threshold;

        System.out.println("Policy: FairSignalRuntimePolicy");
    }

    @Override
    public BoltRuntimeUnit getUnitAndSet(){
        lock.lock();
        BoltRuntimeUnit ret = null;
        try {
           while ((ret = getUnit()) == null) {
                condition.await();
            }

            availableBolts.remove(ret);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }

        return ret;
    }

    @Override
    public void unitReset(BoltRuntimeUnit unit) {
        lock.lock();
        if (unit.getNumInQ() == 0)
            unit.resetReadyTime();
        else
            unit.setReadyTime();
        availableBolts.add(unit);
        lock.unlock();
    }

    @Override
    public void print() {
        System.out.println("Policy: FairSignalRuntimePolicy");
        for (BoltRuntimeUnit bolt : bolts) {
            bolt.print();
        }
    }

    private BoltRuntimeUnit getUnit() {
        BoltRuntimeUnit ret_pending = null, ret_waitted = null, ret = null;
        long max_pending = 0;
        int max_waited = 0;
        long max_waited_pending = 0;

        for (BoltRuntimeUnit bolt : availableBolts) {
            long numInQ = bolt.getNumInQ();
            if (numInQ > max_pending) {
                max_pending = numInQ;
                ret_pending = bolt;
            }

            if (numInQ > 0) {
                int waited_count = bolt.getWaitedCount();
                if (waited_count > max_waited
                        || (waited_count == max_waited && numInQ > max_waited_pending)) {
                    max_waited = waited_count;
                    max_waited_pending = numInQ;
                    ret_waitted = bolt;
                }
            }

            bolt.incWaitedCount();
        }

        if (max_waited > threshold)
            ret = ret_waitted;
        else
            ret = ret_pending;

        if (ret != null)
            ret.resetWaitedCount();

        return ret;
    }
}

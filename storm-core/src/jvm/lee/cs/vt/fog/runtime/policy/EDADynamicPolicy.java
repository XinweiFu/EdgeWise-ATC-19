package lee.cs.vt.fog.runtime.policy;

import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;
import lee.cs.vt.fog.runtime.FogRuntime;
import org.apache.storm.metric.internal.MultiCountStatAndMetric;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class EDADynamicPolicy implements RuntimePolicy {

    private final boolean getWaitTime;
    private final boolean getEmptyTime;

    private final Set<BoltRuntimeUnit> bolts;
    private final Set<BoltRuntimeUnit> availableBolts;

    private final Lock lock = FogRuntime.LOCK;
    private final Condition condition = FogRuntime.CONDITION;

    public EDADynamicPolicy(List<ExecutorCallback.CallbackProvider> list,
                            Map<Object, BoltReceiveDisruptorQueue> map) {
        this.getWaitTime = FogRuntime.getWaitTime;
        this.getEmptyTime = FogRuntime.getEmptyTime;

        this.bolts = new HashSet<BoltRuntimeUnit>();

        for (ExecutorCallback.CallbackProvider provider : list) {
            ExecutorCallback callback = provider.getCallback();
            if (callback == null)
                continue;

            MultiCountStatAndMetric metric = provider.getWaitLatencyMetric();

            Object executorId = callback.getExecutorId();
            BoltReceiveDisruptorQueue queue = map.get(executorId);
            assert(queue != null);

            String componentId = callback.getComponentId();

            ExecutorCallback.ExecutorType type = callback.getType();
            switch (type) {
                case bolt:
                    this.bolts.add(new BoltRuntimeUnit(componentId, queue, callback, metric));
                    break;
                default:
                    // Never comes here
                    assert(false);
            }
        }

        this.availableBolts = new HashSet<BoltRuntimeUnit>(bolts);

        System.out.println("Policy: EDADynamicPolicy");
    }

    @Override
    public BoltRuntimeUnit getUnitAndSet(){
        lock.lock();
        BoltRuntimeUnit ret = null;
        try {
           while ((ret = getUnit()) == null) {
                condition.await();
            }

            if (getWaitTime) {
                ret.addWaitTime();
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
        availableBolts.add(unit);

        if (getWaitTime &&
                unit.getNumInQ() > 0) {
            unit.setWaitStartTime();
        }

        if (getEmptyTime &&
                unit.getNumInQ() == 0) {
            unit.setEmptyStartTime();
        }

        lock.unlock();
    }

    @Override
    public void print() {
        System.out.println("Policy: EDADynamicPolicy");
        for (BoltRuntimeUnit bolt : bolts) {
            bolt.print();
        }
    }

    @Override
    public String printAverageWaitTime() {
        String ret = "";
        for (BoltRuntimeUnit bolt : bolts) {
            ret += bolt.printAverageWaitTime();
        }
        return ret;
    }

    @Override
    public String printTotalEmptyTime() {
        String ret = "";
        for (BoltRuntimeUnit bolt : bolts) {
            ret += bolt.printTotalEmptyTime();
        }
        return ret;
    }

    private BoltRuntimeUnit getUnit() {
        BoltRuntimeUnit ret = null;
        long max = 0;

        for (BoltRuntimeUnit bolt : availableBolts) {
            long numInQ = bolt.getNumInQ();
            if (numInQ > max) {
                max = numInQ;
                ret = bolt;
            }
        }

        return ret;
    }
}

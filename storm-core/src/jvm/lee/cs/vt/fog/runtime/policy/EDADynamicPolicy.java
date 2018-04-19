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
    private final Set<BoltRuntimeUnit> bolts;
    private final Set<BoltRuntimeUnit> availableBolts;

    private final Lock lock = FogRuntime.LOCK;
    private final Condition condition = FogRuntime.CONDITION;

    public EDADynamicPolicy(Set<BoltRuntimeUnit> bolts) {
        this.bolts = bolts;
        this.availableBolts = new HashSet<BoltRuntimeUnit>(bolts);
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
        availableBolts.add(unit);
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

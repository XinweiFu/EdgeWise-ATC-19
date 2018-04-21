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
    private final Set<BoltRuntimeUnit> availables;

    private final Lock lock = FogRuntime.LOCK;
    private final Condition condition = FogRuntime.CONDITION;

    public EDADynamicPolicy(Set<BoltRuntimeUnit> bolts) {
        this.bolts = bolts;
        this.availables = new HashSet<BoltRuntimeUnit>();

        for (BoltRuntimeUnit bolt : bolts) {
            bolt.setPolicy(this);
            bolt.getQueue().setUnit(bolt);
        }
    }

    @Override
    public BoltRuntimeUnit getUnitAndSet(){
        lock.lock();
        BoltRuntimeUnit ret = null;
        try {
           while (availables.isEmpty()) {
                condition.await();
            }

            ret = getUnit();
            assert(ret != null);
            assert (!ret.isRunning());
            ret.setIsRunning(true);
            availables.remove(ret);

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
        assert (unit.isRunning());
        unit.setIsRunning(false);
        if (unit.getNumInQ() > 0 && !availables.contains(unit)) {
            availables.add(unit);
        }
        lock.unlock();
    }

    @Override
    public void updateEmptyQueue(BoltRuntimeUnit unit) {
        if (!unit.isRunning() && !availables.contains(unit)) {
            availables.add(unit);
        }
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

        for (BoltRuntimeUnit bolt : availables) {
            long numInQ = bolt.getNumInQ();
            if (numInQ > max) {
                max = numInQ;
                ret = bolt;
            }
        }

        return ret;
    }
}

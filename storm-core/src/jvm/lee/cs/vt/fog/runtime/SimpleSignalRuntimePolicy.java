package lee.cs.vt.fog.runtime;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class SimpleSignalRuntimePolicy implements RuntimePolicy {

    private final Set<BoltRuntimeUnit> bolts;
    private final Set<BoltRuntimeUnit> availableBolts;

    private final Lock lock = FogRuntime.LOCK;
    private final Condition condition = FogRuntime.CONDITION;

    public SimpleSignalRuntimePolicy(Set<BoltRuntimeUnit> bolts) {
        this.bolts = bolts;
        this.availableBolts = new HashSet<BoltRuntimeUnit>(bolts);
        System.out.println("Policy: SimpleSignalRuntimePolicy");
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
    public synchronized void unitReset(BoltRuntimeUnit unit) {
        lock.lock();
        availableBolts.add(unit);
        lock.unlock();
    }

    private BoltRuntimeUnit getUnit() {
        BoltRuntimeUnit ret = null;
        long max = 0;

        for(BoltRuntimeUnit bolt : availableBolts) {
            long numInQ = bolt.getNumInQ();
            if (numInQ > max) {
                max = numInQ;
                ret = bolt;
            }
        }

        return ret;
    }
}

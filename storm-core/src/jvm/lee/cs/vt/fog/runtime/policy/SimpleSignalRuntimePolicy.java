package lee.cs.vt.fog.runtime.policy;

import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;
import lee.cs.vt.fog.runtime.FogRuntime;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnitGroup;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class SimpleSignalRuntimePolicy implements RuntimePolicy {

    private final Set<BoltRuntimeUnitGroup> groups;
    private final Set<BoltRuntimeUnit> availableBolts;

    private final Lock lock = FogRuntime.LOCK;
    private final Condition condition = FogRuntime.CONDITION;

    public SimpleSignalRuntimePolicy(Set<BoltRuntimeUnitGroup> groups) {
        this.groups = groups;
        this.availableBolts = new HashSet<BoltRuntimeUnit>();

        for (BoltRuntimeUnitGroup group : groups) {
            availableBolts.addAll(group.getUnits());
        }

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
    public void unitReset(BoltRuntimeUnit unit) {
        lock.lock();
        availableBolts.add(unit);
        lock.unlock();
    }

    private BoltRuntimeUnit getUnit() {
        BoltRuntimeUnit ret = null;
        long max = 0;

        for (BoltRuntimeUnitGroup group : groups) {
            long numInQ = group.getTotalNumInQ();
            if (numInQ > max) {
                max = numInQ;
                ret = group.getUnitWithMaxNumInQ();
            }
        }

        return ret;
    }
}

package lee.cs.vt.fog.runtime.misc;

import lee.cs.vt.fog.runtime.FogRuntime;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;
import lee.cs.vt.fog.runtime.unit.WeightBoltRuntimeUnit;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class RandomWeightManager implements WeightManager{
    private final Lock lock = FogRuntime.LOCK;
    private final Condition condition = FogRuntime.CONDITION;

    private final Set<WeightBoltRuntimeUnit> bolts;
    private final List<WeightBoltRuntimeUnit> availables = new ArrayList<WeightBoltRuntimeUnit>();

    private Random ran = new Random();

    public RandomWeightManager(Set<BoltRuntimeUnit> bolts) {
        this.bolts = new HashSet<WeightBoltRuntimeUnit>();
        initBolts(bolts);

        print();
    }

    @Override
    public BoltRuntimeUnit getUnitAndSet() {
        lock.lock();
        WeightBoltRuntimeUnit ret = null;

        try {
            while (availables.isEmpty()) {
                condition.await();
            }

            int i = ran.nextInt(availables.size());
            ret = availables.get(i);
            assert(ret != null);
            availables.remove(i);
            ret.setIsRunning(true);

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
        WeightBoltRuntimeUnit weightUnit = (WeightBoltRuntimeUnit) unit;
        weightUnit.setIsRunning(false);
        if (unit.getNumInQ() > 0) {
            availables.add(weightUnit);
        }
        lock.unlock();
    }

    @Override
    public void updateEmptyQueue(WeightBoltRuntimeUnit unit) {
        if (!unit.isRunning()) {
            availables.add(unit);
        }
    }

    @Override
    public void print() {
        System.out.println("RandomWeightManager:");
        for (WeightBoltRuntimeUnit unit : bolts) {
            unit.print();
        }
    }

    private void initBolts(Set<BoltRuntimeUnit> bolts) {
        for (BoltRuntimeUnit bolt : bolts) {
            String id = bolt.getComponentId();
            BoltReceiveDisruptorQueue queue = bolt.getQueue();
            ExecutorCallback callback = bolt.getCallback();
            int weight = 1;

            WeightBoltRuntimeUnit unit = new WeightBoltRuntimeUnit(id, queue, callback, weight, this);
            queue.setWeightUnit(unit);
            this.bolts.add(unit);
        }
    }
}
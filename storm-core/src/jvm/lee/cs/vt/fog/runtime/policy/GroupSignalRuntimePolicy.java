package lee.cs.vt.fog.runtime.policy;

import lee.cs.vt.fog.runtime.FogRuntime;
import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.unit.AdaptBoltRuntimeUnitGroup;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnitGroup;
import lee.cs.vt.fog.runtime.unit.SingleBoltRuntimeUnitGroup;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class GroupSignalRuntimePolicy implements RuntimePolicy {

    private final Set<BoltRuntimeUnitGroup> groups;

    private final Map<BoltRuntimeUnit, BoltRuntimeUnitGroup> unit2GroupMap;

    private final Lock lock = FogRuntime.LOCK;
    private final Condition condition = FogRuntime.CONDITION;

    public GroupSignalRuntimePolicy(List<ExecutorCallback.CallbackProvider> list,
                                    Map<Object, BoltReceiveDisruptorQueue> map) {
        Map<String, Set<BoltRuntimeUnit>> unitsMap = new HashMap<String,Set<BoltRuntimeUnit>>();

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
                    if (!unitsMap.containsKey(componentId))
                        unitsMap.put(componentId, new HashSet<BoltRuntimeUnit>());
                    unitsMap.get(componentId).add(new BoltRuntimeUnit(componentId, queue, callback));
                    break;
                default:
                    // Never comes here
                    assert(false);
            }
        }

        this.groups = new HashSet<BoltRuntimeUnitGroup>();
        this.unit2GroupMap = new HashMap<BoltRuntimeUnit, BoltRuntimeUnitGroup>();

        for (Set<BoltRuntimeUnit> unitSet : unitsMap.values()) {
            int size = unitSet.size();
            if (size == 1) {
                BoltRuntimeUnit unit = unitSet.iterator().next();
                SingleBoltRuntimeUnitGroup group = new SingleBoltRuntimeUnitGroup(unit);
                groups.add(group);
                unit2GroupMap.put(unit, group);
            } else{
                AdaptBoltRuntimeUnitGroup group = new AdaptBoltRuntimeUnitGroup(unitSet);
                groups.add(group);
                for (BoltRuntimeUnit unit : unitSet) {
                    unit2GroupMap.put(unit, group);
                }
            }
        }

        System.out.println("Policy: GroupSignalRuntimePolicy");
    }

    @Override
    public BoltRuntimeUnit getUnitAndSet(){
        lock.lock();
        BoltRuntimeUnit ret = null;
        try {
            while ((ret = getUnit()) == null) {
                condition.await();
            }

            BoltRuntimeUnitGroup group = unit2GroupMap.get(ret);
            group.setUnavailable(ret);

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
        BoltRuntimeUnitGroup group = unit2GroupMap.get(unit);
        group.setAvailable(unit);
        lock.unlock();
    }

    @Override
    public void print() {
        System.out.println("Policy: GroupSignalRuntimePolicy");

        for (BoltRuntimeUnitGroup group : groups) {
            group.print();
        }
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

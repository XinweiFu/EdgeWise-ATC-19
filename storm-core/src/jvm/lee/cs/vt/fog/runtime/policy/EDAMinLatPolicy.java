package lee.cs.vt.fog.runtime.policy;

import lee.cs.vt.fog.runtime.FogRuntime;
import lee.cs.vt.fog.runtime.misc.BoltReceiveDisruptorQueue;
import lee.cs.vt.fog.runtime.misc.ExecutorCallback;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;
import lee.cs.vt.fog.runtime.unit.WeightBoltRuntimeUnit;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class EDAMinLatPolicy implements RuntimePolicy {
    private final Set<WeightBoltRuntimeUnit> bolts;
    private final Set<WeightBoltRuntimeUnit> availables;

    private final Lock lock = FogRuntime.LOCK;
    private final Condition condition = FogRuntime.CONDITION;

    public EDAMinLatPolicy(Set<BoltRuntimeUnit> bolts,
                           Map storm_conf){
        Map<String, Integer> prios = getPrios(storm_conf);

        this.bolts = new HashSet<WeightBoltRuntimeUnit>();
        initBolts(bolts, prios);

        this.availables = new HashSet<WeightBoltRuntimeUnit>();
        this.availables.addAll(this.bolts);

        print();
    }

    @Override
    public BoltRuntimeUnit getUnitAndSet() {
        lock.lock();
        BoltRuntimeUnit ret = null;
        try {
            while ((ret = getUnit()) == null) {
                condition.await();
            }

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
        availables.add((WeightBoltRuntimeUnit) unit);
        lock.unlock();
    }

    @Override
    public void updateEmptyQueue(BoltRuntimeUnit unit) {

    }

    @Override
    public void print() {
        System.out.println("EDAMinLatPolicy:");
        for (WeightBoltRuntimeUnit unit : bolts) {
            unit.print();
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
        long prio_max = -1;
        BoltRuntimeUnit ret = null;

        for (WeightBoltRuntimeUnit bolt : availables) {
            if (bolt.getNumInQ() == 0) {
                continue;
            }

            long prio = bolt.getWeight();
            if (prio > prio_max) {
                prio_max = prio;
                ret = bolt;
            }
        }

        return ret;
    }

    private Map<String, Integer> getPrios(Map storm_conf) {
        List<String> ids = getStrs((String) storm_conf.get("min-lat-bolt-ids"));
        List<Integer> prios = getInts((String) storm_conf.get("min-lat-bolt-prios"));

        int n = ids.size();
        assert(n > 0);
        assert(prios.size() == n);

        Map<String, Integer> prioMap = new HashMap<String, Integer>();
        for (int i = 0; i < n; i++) {
            String id = ids.get(i);
            int prio = prios.get(i);
            prioMap.put(id, prio);
        }

        System.out.println("prioMap = " + prioMap);

        return prioMap;
    }

    private void initBolts(Set<BoltRuntimeUnit> bolts,
                           Map<String, Integer> prios) {
        for (BoltRuntimeUnit bolt : bolts) {
            String id = bolt.getComponentId();
            BoltReceiveDisruptorQueue queue = bolt.getQueue();
            ExecutorCallback callback = bolt.getCallback();

            System.out.println("id = " + id);

            int prio = prios.get(id);

            WeightBoltRuntimeUnit unit = new WeightBoltRuntimeUnit(id, queue, callback, prio);
            unit.setPolicy(this);
            queue.setUnit(unit);
            this.bolts.add(unit);
        }
    }

    private List<Integer> getInts(String s) {
        String[] list = s.split(",");
        List<Integer> ret = new ArrayList<Integer>();
        for (String str : list) {
            ret.add(Integer.parseInt(str));
        }
        return ret;
    }

    private List<String> getStrs(String s) {
        String[] list = s.split(",");
        List<String> ret = new ArrayList<String>();
        for (String str : list) {
            ret.add(str);
        }
        return ret;
    }
}

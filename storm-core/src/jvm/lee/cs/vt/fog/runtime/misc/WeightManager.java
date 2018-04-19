package lee.cs.vt.fog.runtime.misc;

import lee.cs.vt.fog.runtime.FogRuntime;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;
import lee.cs.vt.fog.runtime.unit.WeightBoltRuntimeUnit;

import java.util.*;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class WeightManager {
    private final Lock lock = FogRuntime.LOCK;
    private final Condition condition = FogRuntime.CONDITION;

    private final Set<WeightBoltRuntimeUnit> bolts;
    private final List<WeightBoltRuntimeUnit> availables = new ArrayList<WeightBoltRuntimeUnit>();
    private int availableWeight = 0;

    private Random ran = new Random();

    public WeightManager(Set<BoltRuntimeUnit> bolts,
                         Map storm_conf) {
        Map<String, Integer> weights = getWeights(storm_conf);

        this.bolts = new HashSet<WeightBoltRuntimeUnit>();
        initBolts(bolts, weights);

        print();
    }

    public BoltRuntimeUnit getUnitAndSet() {
        lock.lock();
        WeightBoltRuntimeUnit ret = null;

        try {
            while (availableWeight == 0) {
                condition.await();
            }

            int target = ran.nextInt(availableWeight) + 1;
            int sum = 0;
            int i = 0;
            for (; i < availables.size(); i++) {
                WeightBoltRuntimeUnit unit = availables.get(i);
                sum += unit.getWeight();
                if (sum >= target) {
                    ret = unit;
                    break;
                }
            }

            assert(i < availables.size() && ret != null);
            availables.remove(i);
            availableWeight -= ret.getWeight();
            ret.setIsRunning(true);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }

        return ret;
    }

    public void unitReset(BoltRuntimeUnit unit) {
        lock.lock();
        WeightBoltRuntimeUnit weightUnit = (WeightBoltRuntimeUnit) unit;
        weightUnit.setIsRunning(false);
        if (unit.getNumInQ() > 0) {
            availables.add(weightUnit);
            availableWeight += weightUnit.getWeight();
        }
        lock.unlock();
    }

    public void updateEmptyQueue(WeightBoltRuntimeUnit unit) {
        if (!unit.isRunning()) {
            availables.add(unit);
            availableWeight += unit.getWeight();
        }
    }

    public void print() {
        System.out.println("Policy: WeightManager");
        for (WeightBoltRuntimeUnit unit : bolts) {
            unit.print();
        }
        System.out.println("availableWeight:" + availableWeight);
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

    private Map<String, Integer> getWeights(Map storm_conf) {
        List<String> ids = getStrs((String) storm_conf.get("static-bolt-ids"));
        List<Integer> weights = getInts((String) storm_conf.get("static-bolt-weights"));

        int n = ids.size();
        assert(n > 0);
        assert(weights.size() == n);

        Map<String, Integer> weightMap = new HashMap<String, Integer>();
        for (int i = 0; i < n; i++) {
            String id = ids.get(i);
            int weight = weights.get(i);
            weightMap.put(id, weight);
        }
        return weightMap;
    }

    private void initBolts(Set<BoltRuntimeUnit> bolts,
                           Map<String, Integer> weights) {
        for (BoltRuntimeUnit bolt : bolts) {
            String id = bolt.getComponentId();
            BoltReceiveDisruptorQueue queue = bolt.getQueue();
            ExecutorCallback callback = bolt.getCallback();
            int weight = weights.get(id);

            WeightBoltRuntimeUnit unit = new WeightBoltRuntimeUnit(id, queue, callback, weight, this);
            queue.setWeightUnit(unit);
            this.bolts.add(unit);
        }
    }
}
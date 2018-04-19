package lee.cs.vt.fog.runtime.policy;

import lee.cs.vt.fog.runtime.misc.WeightManager;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;

import java.util.Map;
import java.util.Set;

public class EDAStaticPolicy implements RuntimePolicy {
    private final Set<BoltRuntimeUnit> bolts;
    private final WeightManager weightManager;

    public EDAStaticPolicy(Set<BoltRuntimeUnit> bolts,
                           Map storm_conf) {
        this.bolts = bolts;
        this.weightManager = new WeightManager(bolts, storm_conf);
    }

    @Override
    public BoltRuntimeUnit getUnitAndSet() {
        return weightManager.getUnitAndSet();
    }

    @Override
    public void unitReset(BoltRuntimeUnit unit) {
        weightManager.unitReset(unit);
    }

    @Override
    public void print() {
        System.out.println("Policy: EDAStaticPolicy");
        weightManager.print();
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
}
package lee.cs.vt.fog.runtime.policy;

import lee.cs.vt.fog.runtime.misc.RandomWeightManager;
import lee.cs.vt.fog.runtime.misc.WeightManager;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;

import java.util.Map;
import java.util.Set;

public class EDARandomPolicy extends EDAWeightPolicy {
    public EDARandomPolicy(Set<BoltRuntimeUnit> bolts) {
        super(bolts);
    }

    @Override
    public void print() {
        System.out.println("Policy: EDARandomPolicy");
        super.print();
    }

    @Override
    protected WeightManager getManager(Set<BoltRuntimeUnit> bolts, Map storm_conf) {
        return new RandomWeightManager(bolts);
    }
}

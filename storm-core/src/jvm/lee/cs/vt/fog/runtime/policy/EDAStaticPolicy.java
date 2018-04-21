package lee.cs.vt.fog.runtime.policy;

import lee.cs.vt.fog.runtime.misc.StaticWeightManager;
import lee.cs.vt.fog.runtime.misc.WeightManager;
import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;

import java.util.Map;
import java.util.Set;

public class EDAStaticPolicy extends EDAWeightPolicy {
    public EDAStaticPolicy(Set<BoltRuntimeUnit> bolts,
                           Map storm_conf) {
        super(bolts, storm_conf);
    }

    @Override
    public void print() {
        System.out.println("Policy: EDAStaticPolicy");
        super.print();
    }

    @Override
    protected WeightManager getManager(Set<BoltRuntimeUnit> bolts, Map storm_conf) {
        return new StaticWeightManager(bolts, storm_conf, this);
    }
}
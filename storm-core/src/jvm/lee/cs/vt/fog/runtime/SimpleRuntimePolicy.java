package lee.cs.vt.fog.runtime;

import java.util.HashSet;
import java.util.Set;

public class SimpleRuntimePolicy implements RuntimePolicy {

    private final Set<BoltRuntimeUnit> bolts;
    private final Set<BoltRuntimeUnit> availableBolts;

    public SimpleRuntimePolicy(Set<BoltRuntimeUnit> bolts) {
        this.bolts = bolts;
        this.availableBolts = new HashSet<BoltRuntimeUnit>(bolts);
        System.out.println("Policy: SimpleRuntimePolicy");
    }

    @Override
    public synchronized BoltRuntimeUnit getUnitAndSet() {
        BoltRuntimeUnit ret = null;
        long max = 0;

        for(BoltRuntimeUnit bolt : availableBolts) {
            long numInQ = bolt.getNumInQ();
            if (numInQ > max) {
                max = numInQ;
                ret = bolt;
            }
        }

        if (ret != null)
            availableBolts.remove(ret);

        return ret;
    }

    @Override
    public synchronized void unitReset(BoltRuntimeUnit unit) {
        availableBolts.add(unit);
    }

}

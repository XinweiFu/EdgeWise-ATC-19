package lee.cs.vt.fog.runtime;

import java.util.Set;

public class SimpleRuntimePolicy implements RuntimePolicy {

    private final Set<BoltRuntimeUnit> bolts;

    public SimpleRuntimePolicy(Set<BoltRuntimeUnit> bolts) {
        this.bolts = bolts;
        System.out.println("Policy: SimpleRuntimePolicy");
    }

    @Override
    public synchronized BoltRuntimeUnit getUnitAndSet() {
        BoltRuntimeUnit ret = null;
        long max = 0;

        for(BoltRuntimeUnit bolt : bolts) {
            if (bolt.isRunning())
                continue;

            long numInQ = bolt.getNumInQ();
            if (numInQ > max) {
                max = numInQ;
                ret = bolt;
            }
        }

        if (ret != null)
            ret.setIsRunning();

        return ret;
    }

}

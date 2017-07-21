package lee.cs.vt.fog.runtime;

import java.util.Set;

public class RuntimePolicy {
    private final Set<BoltRuntimeUnit> bolts;

    public RuntimePolicy (Set<BoltRuntimeUnit> bolts) {
        this.bolts = bolts;
    }

    public synchronized BoltRuntimeUnit getUnitAndSet(){
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

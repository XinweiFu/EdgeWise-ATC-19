package lee.cs.vt.fog.runtime.thread;

import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;
import lee.cs.vt.fog.runtime.policy.RuntimePolicy;

import java.util.*;

public class BoltThread extends FogRuntimeThread {
    private final RuntimePolicy policy;

    public BoltThread(RuntimePolicy policy) {
        this.policy = policy;
    }

    @Override
    public void executeUnit() {
        BoltRuntimeUnit runtimeUnit = policy.getUnitAndSet();
        if (runtimeUnit != null) {
            runtimeUnit.run();
            policy.unitReset(runtimeUnit);
        }
    }
}

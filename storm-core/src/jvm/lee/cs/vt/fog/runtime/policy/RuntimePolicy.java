package lee.cs.vt.fog.runtime.policy;

import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;

public interface RuntimePolicy {
    public BoltRuntimeUnit getUnitAndSet();
    public void unitReset(BoltRuntimeUnit unit);
}

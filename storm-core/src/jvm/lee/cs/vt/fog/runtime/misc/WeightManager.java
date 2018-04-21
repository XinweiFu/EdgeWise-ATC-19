package lee.cs.vt.fog.runtime.misc;

import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;

public interface WeightManager {
    public BoltRuntimeUnit getUnitAndSet();
    public void unitReset(BoltRuntimeUnit unit);
    public void updateEmptyQueue(BoltRuntimeUnit unit);
    public void print();
}
package lee.cs.vt.fog.runtime.misc;

import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;
import lee.cs.vt.fog.runtime.unit.WeightBoltRuntimeUnit;

public interface WeightManager {
    public BoltRuntimeUnit getUnitAndSet();
    public void unitReset(BoltRuntimeUnit unit);
    public void updateEmptyQueue(WeightBoltRuntimeUnit unit);
    public void print();
}
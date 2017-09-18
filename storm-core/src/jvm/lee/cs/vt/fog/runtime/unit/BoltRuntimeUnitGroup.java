package lee.cs.vt.fog.runtime.unit;

import java.util.Set;

public interface BoltRuntimeUnitGroup {
    public long getTotalNumInQ();
    public BoltRuntimeUnit getUnitWithMaxNumInQ();
    public Set<BoltRuntimeUnit> getUnits();
    public void print();

}

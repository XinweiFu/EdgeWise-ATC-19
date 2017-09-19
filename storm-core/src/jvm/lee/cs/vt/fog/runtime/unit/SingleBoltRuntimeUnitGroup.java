package lee.cs.vt.fog.runtime.unit;

import java.util.HashSet;
import java.util.Set;

public class SingleBoltRuntimeUnitGroup implements BoltRuntimeUnitGroup{

    private final BoltRuntimeUnit unit;
    private final Set<BoltRuntimeUnit> unitSet = new HashSet<BoltRuntimeUnit>();

    public SingleBoltRuntimeUnitGroup(BoltRuntimeUnit unit) {
        this.unit = unit;
        unitSet.add(unit);
    }

    @Override
    public long getTotalNumInQ() {
        return unit.getNumInQ();
    }

    @Override
    public BoltRuntimeUnit getUnitWithMaxNumInQ() {
        return unit;
    }

    @Override
    public Set<BoltRuntimeUnit> getUnits() {
        return unitSet;
    }

    @Override
    public void setAvailable(BoltRuntimeUnit unit) {

    }

    @Override
    public void setUnavailable(BoltRuntimeUnit unit) {

    }

    @Override
    public void print(){
        System.out.println("SingleBoltRuntimeUnitGroup:");
        unit.print();
    }
}

package lee.cs.vt.fog.runtime.unit;

import java.util.HashSet;
import java.util.Set;

public class SingleBoltRuntimeUnitGroup implements BoltRuntimeUnitGroup{

    private final BoltRuntimeUnit unit;
    private final Set<BoltRuntimeUnit> unitSet = new HashSet<BoltRuntimeUnit>();

    private boolean available;

    public SingleBoltRuntimeUnitGroup(BoltRuntimeUnit unit) {
        this.unit = unit;
        unitSet.add(unit);

        available = true;
    }

    @Override
    public long getTotalNumInQ() {
        if (available)
            return unit.getNumInQ();
        else
            return 0;
    }

    @Override
    public BoltRuntimeUnit getUnitWithMaxNumInQ() {
        if (available)
            return unit;
        else
            return null;
    }

    @Override
    public Set<BoltRuntimeUnit> getUnits() {
        return unitSet;
    }

    @Override
    public void setAvailable(BoltRuntimeUnit unit) {
        available = true;
    }

    @Override
    public void setUnavailable(BoltRuntimeUnit unit) {
        available =false;
    }

    @Override
    public void print(){
        System.out.println("SingleBoltRuntimeUnitGroup:");
        unit.print();
    }
}

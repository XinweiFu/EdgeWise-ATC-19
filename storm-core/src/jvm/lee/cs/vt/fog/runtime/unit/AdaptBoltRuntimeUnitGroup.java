package lee.cs.vt.fog.runtime.unit;

import java.util.HashSet;
import java.util.Set;

public class AdaptBoltRuntimeUnitGroup implements BoltRuntimeUnitGroup{
    private final Set<BoltRuntimeUnit> boltUnits = new HashSet<BoltRuntimeUnit>();
    private final Set<BoltRuntimeUnit> availableBoltUnits = new HashSet<BoltRuntimeUnit>();

    private BoltRuntimeUnit unitWithMaxNumInQ;

    public AdaptBoltRuntimeUnitGroup(Set<BoltRuntimeUnit> boltUnits) {
        this.boltUnits.addAll(boltUnits);
        this.availableBoltUnits.addAll(boltUnits);
    }

    @Override
    public long getTotalNumInQ() {
        long totalNum = 0;
        long maxNum = 0;

        for (BoltRuntimeUnit boltUnit : availableBoltUnits) {
            long num = boltUnit.getNumInQ();

            totalNum += num;

            if (num > maxNum) {
                maxNum = num;
                unitWithMaxNumInQ = boltUnit;
            }
        }

        return totalNum;
    }

    @Override
    public BoltRuntimeUnit getUnitWithMaxNumInQ() {
        assert (unitWithMaxNumInQ != null);
        return unitWithMaxNumInQ;
    }

    @Override
    public Set<BoltRuntimeUnit> getUnits() {
        return boltUnits;
    }

    @Override
    public void setAvailable(BoltRuntimeUnit unit) {
        availableBoltUnits.add(unit);
    }

    @Override
    public void setUnavailable(BoltRuntimeUnit unit) {
        availableBoltUnits.remove(unit);
    }

    @Override
    public void print(){
        System.out.println("AdaptBoltRuntimeUnitGroup:");
        for (BoltRuntimeUnit boltUnit : boltUnits) {
            boltUnit.print();
        }
    }
}



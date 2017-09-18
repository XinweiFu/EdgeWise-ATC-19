package lee.cs.vt.fog.runtime.unit;

import java.util.HashSet;
import java.util.Set;

public class AdaptBoltRuntimeUnitGroup implements BoltRuntimeUnitGroup{
    private final Set<BoltRuntimeUnit> boltUnits = new HashSet<BoltRuntimeUnit>();

    private BoltRuntimeUnit unitWithMaxNumInQ;

    public AdaptBoltRuntimeUnitGroup(Set<BoltRuntimeUnit> boltUnits) {
        this.boltUnits.addAll(boltUnits);
    }

    public void add(BoltRuntimeUnit unit) {
        boltUnits.add(unit);
    }

    @Override
    public long getTotalNumInQ() {
        long totalNum = 0;
        long maxNum = 0;

        for (BoltRuntimeUnit boltUnit : boltUnits) {
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
    public void print(){
        System.out.println("SingleBoltRuntimeUnitGroup: " + boltUnits.iterator().next().getComponentId());
    }
}



package lee.cs.vt.fog.runtime.unit;

import java.util.*;

public class BoltRuntimeUnitContainer {
    private final Set<BoltRuntimeUnitGroup> groupSet = new HashSet<BoltRuntimeUnitGroup>();

    public BoltRuntimeUnitContainer(Collection<Set<BoltRuntimeUnit>> unitSets, int numThreadPoll) {
        for (Set<BoltRuntimeUnit> unitSet : unitSets) {
            int size = unitSet.size();
            if (size == 1) {
                BoltRuntimeUnit unit = unitSet.iterator().next();
                SingleBoltRuntimeUnitGroup group = new SingleBoltRuntimeUnitGroup(unit);
                groupSet.add(group);
            } else if (size == numThreadPoll) {
                AdaptBoltRuntimeUnitGroup group = new AdaptBoltRuntimeUnitGroup(unitSet);
                groupSet.add(group);
            } else {
                // Never come here
                assert (false);
            }
        }
    }

    public Set<BoltRuntimeUnitGroup> getGroups() {
        return groupSet;
    }
}

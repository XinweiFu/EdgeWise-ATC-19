package lee.cs.vt.fog.runtime.policy;

import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Set;

// Deprecated for adaptive instances

public class RankingRuntimePolicy {
        /*implements RuntimePolicy {
    private final Set<BoltRuntimeUnit> bolts;
    private final int numThreadPoll;
    private final Comparator<Pair> comparator = new Comparator<Pair>(){
        @Override
        public int compare(Pair arg0, Pair arg1) {
            long num0 = arg0.getNum();
            long num1 = arg1.getNum();
            if (num0 < num1)
                return 1;
            else if (num0 > num1)
                return -1;
            else
                return 0;
        }
    };

    public RankingRuntimePolicy(Set<BoltRuntimeUnit> bolts, int numThreadPoll) {
        this.bolts = bolts;
        this.numThreadPoll = numThreadPoll;
        System.out.println("Policy: RankingRuntimePolicy");
    }

    @Override
    public BoltRuntimeUnit getUnitAndSet(){

        PriorityQueue<Pair> queue = new PriorityQueue<Pair>(numThreadPoll, comparator);

        for(BoltRuntimeUnit bolt : bolts) {
            long num = bolt.getNumInQ();
            if (num == 0)
                continue;

            Pair p = new Pair(bolt, num);
            queue.add(p);

        }

        for(int i = 0; i < numThreadPoll; i++) {
            Pair p = queue.poll();
            if (p == null)
                return null;

            BoltRuntimeUnit unit = p.getUnit();

            if (unit.checkAndSet()) {
                return unit;
            }
        }

        return null;
    }

    @Override
    public void unitReset(BoltRuntimeUnit unit) {
        unit.resetIsRunning();
    }

    private class Pair {
        private final BoltRuntimeUnit unit;
        private final long num;

        Pair(BoltRuntimeUnit unit, long num) {
            this.unit = unit;
            this.num = num;
        }

        public BoltRuntimeUnit getUnit() {
            return unit;
        }

        public long getNum() {
            return num;
        }
    }
    */
}

package lee.cs.vt.fog.runtime.thread;

import lee.cs.vt.fog.runtime.unit.BoltRuntimeUnit;
import lee.cs.vt.fog.runtime.policy.RuntimePolicy;

import java.util.*;

public class BoltThread extends FogRuntimeThread {
    private final RuntimePolicy policy;
    private final boolean debug;
    private final List<String> debug_info;

    private final Map<BoltRuntimeUnit, Long> turnMap;
    private final Map<BoltRuntimeUnit, Long> consumeTimeMap;
    private final Map<BoltRuntimeUnit, Set<Long>> waitTimeMap;

    public BoltThread(RuntimePolicy policy, boolean debug, List<String> debug_info) {
        this.policy = policy;
        this.debug = debug;
        this.debug_info = debug_info;

        turnMap = new HashMap<BoltRuntimeUnit, Long>();
        consumeTimeMap = new HashMap<BoltRuntimeUnit, Long>();;
        waitTimeMap = new HashMap<BoltRuntimeUnit, Set<Long>>();;

    }

    @Override
    public void executeUnit() {
        BoltRuntimeUnit runtimeUnit = policy.getUnitAndSet();
        if (runtimeUnit != null) {
            long startTime = 0;

            if (debug) {
                String s = Thread.currentThread().getName();
                s += ",";
                s += runtimeUnit.getName();
                debug_info.add(s);

                if (!turnMap.containsKey(runtimeUnit))
                    turnMap.put(runtimeUnit, 0L);
                turnMap.put(runtimeUnit, turnMap.get(runtimeUnit).longValue() + 1);

                Long waitedTime = runtimeUnit.getWaitedTime();
                if (waitedTime != null) {
                    if (!waitTimeMap.containsKey(runtimeUnit))
                        waitTimeMap.put(runtimeUnit, new HashSet<Long>());
                    waitTimeMap.get(runtimeUnit).add(waitedTime);
                }

                startTime = System.nanoTime();
            }

            runtimeUnit.run();

            if (debug) {
                long endTime = System.nanoTime();

                assert (startTime != 0);
                long consumeTime = endTime - startTime;

                if (!consumeTimeMap.containsKey(runtimeUnit))
                    consumeTimeMap.put(runtimeUnit, 0L);
                consumeTimeMap.put(runtimeUnit, consumeTimeMap.get(runtimeUnit).longValue() + consumeTime);

                runtimeUnit.setCompletedTime();
            }
            policy.unitReset(runtimeUnit);
        }
    }

    public Map<BoltRuntimeUnit, Long> getTurnMap() {
        return turnMap;
    }

    public Map<BoltRuntimeUnit, Long> getConsumeTimeMap() {
        return consumeTimeMap;
    }

    public Map<BoltRuntimeUnit, Set<Long>> getWaitTimeMap() {
        return waitTimeMap;
    }
}

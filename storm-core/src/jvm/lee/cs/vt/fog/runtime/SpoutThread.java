package lee.cs.vt.fog.runtime;

import java.util.Set;

public class SpoutThread extends FogRuntimeThread{
    private final Set<ExecutorCallback> spouts;

    public SpoutThread(Set<ExecutorCallback> spouts) {
        this.spouts = spouts;
    }

    @Override
    public void executeUnit() {
        boolean atLeastOneEmitted = false;
        for(ExecutorCallback spout : spouts) {
            boolean notEmitted = (boolean) spout.run();
            atLeastOneEmitted = atLeastOneEmitted || (!notEmitted);
        }

        if(!atLeastOneEmitted) {
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}

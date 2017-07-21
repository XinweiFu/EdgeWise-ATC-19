package lee.cs.vt.fog.runtime;

public class BoltThread extends FogRuntimeThread{
    private final RuntimePolicy policy;

    public BoltThread(RuntimePolicy policy) {
        this.policy = policy;
    }

    @Override
    public void executeUnit() {
        RuntimeUnit runtimeUnit = policy.getUnitAndSet();
        if (runtimeUnit != null)
            runtimeUnit.runAndReset();
    }
}

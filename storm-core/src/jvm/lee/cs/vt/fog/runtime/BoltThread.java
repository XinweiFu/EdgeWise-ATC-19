package lee.cs.vt.fog.runtime;

public class BoltThread extends FogRuntimeThread{
    private final RuntimePolicy policy;

    public BoltThread(RuntimePolicy policy) {
        this.policy = policy;
    }

    @Override
    public void executeUnit() {
        BoltRuntimeUnit runtimeUnit = policy.getUnitAndSet();
        if (runtimeUnit != null) {
            runtimeUnit.run();
            policy.unitReset(runtimeUnit);
        }
    }
}

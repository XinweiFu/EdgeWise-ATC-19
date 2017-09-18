package lee.cs.vt.fog.runtime.misc;

public interface ExecutorCallback {
    public Object run();
    public ExecutorType getType();
    public Object getExecutorId();
    public String getComponentId();

    public enum ExecutorType {
        spout,
        bolt,
    }

    public interface CallbackProvider {
        public ExecutorCallback getCallback();
    }
}

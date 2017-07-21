package lee.cs.vt.fog.runtime;

public interface ExecutorCallback {
    public Object run();
    public ExecutorType getType();
    public Object getExecutorId();

    public enum ExecutorType {
        spout,
        bolt,
    }

    public interface CallbackProvider {
        public ExecutorCallback getCallback();
    }
}

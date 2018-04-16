package lee.cs.vt.fog.runtime.misc;

import org.apache.storm.metric.internal.MultiCountStatAndMetric;

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
        public MultiCountStatAndMetric getWaitLatencyMetric();
    }
}

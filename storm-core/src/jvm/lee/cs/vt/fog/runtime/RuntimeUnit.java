package lee.cs.vt.fog.runtime;

import java.util.concurrent.atomic.AtomicBoolean;

public class RuntimeUnit {
    private final ExecutorCallback callback;
    private final AtomicBoolean isRunning;

    public RuntimeUnit (ExecutorCallback callback) {
        this.callback = callback;
        isRunning = new AtomicBoolean();
    }

    public void runAndReset() {
        callback.run();
        isRunning.set(false);
    }

    public boolean isRunning() {
        return isRunning.get();
    }

    public void setIsRunning() {
        isRunning.set(true);
    }

    public ExecutorCallback getCallback() {
        return callback;
    }
}

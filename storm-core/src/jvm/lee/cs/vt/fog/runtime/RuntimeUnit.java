package lee.cs.vt.fog.runtime;

public class RuntimeUnit {
    private final ExecutorCallback callback;
    private boolean isRunning = false;

    public RuntimeUnit (ExecutorCallback callback) {
        this.callback = callback;
    }

    public void runAndReset() {
        callback.run();
        isRunning = false;
    }

    public ExecutorCallback getCallback() {
        return callback;
    }

    public synchronized boolean checkAndSet() {
        if(!isRunning) {
            isRunning = true;
            return true;
        } else {
            return false;
        }
    }

    public boolean isRunning() {
        return isRunning;
    }

    public void setIsRunning() {
        isRunning = true;
    }
}

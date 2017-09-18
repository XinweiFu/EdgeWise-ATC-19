package lee.cs.vt.fog.runtime.unit;

import lee.cs.vt.fog.runtime.misc.ExecutorCallback;

public class RuntimeUnit {
    private final ExecutorCallback callback;

    public RuntimeUnit (ExecutorCallback callback) {
        this.callback = callback;
    }


    public void run() {
        callback.run();
    }

    public ExecutorCallback getCallback() {
        return callback;
    }
}

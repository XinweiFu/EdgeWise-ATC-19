package lee.cs.vt.fog.runtime.thread;

public abstract class FogRuntimeThread extends Thread {

    private boolean running = true;

    @Override
    public void run () {
        while(running) {
            executeUnit();
        }
    }

    public void stopAndWait() throws InterruptedException {
        running = false;
        this.join();
    }

    public abstract void executeUnit();

}

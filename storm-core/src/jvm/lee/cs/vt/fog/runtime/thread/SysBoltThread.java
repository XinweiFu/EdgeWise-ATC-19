package lee.cs.vt.fog.runtime.thread;

import lee.cs.vt.fog.runtime.unit.SysBoltRuntimeUnit;

public class SysBoltThread extends FogRuntimeThread {

    private final SysBoltRuntimeUnit unit;

    public SysBoltThread (SysBoltRuntimeUnit unit) {
        this.unit = unit;
    }

    @Override
    public void executeUnit() {
        unit.run();
    }

    public void print() {
        unit.print();
    }
}
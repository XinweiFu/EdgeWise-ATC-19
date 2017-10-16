package lee.cs.vt.fog.runtime.misc;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;

public class BPCounter {

    private long last_backpressure_on = -1;
    private long backpressure_time = 0;
    private boolean backpressure_flag = false;

    public void backpressure_on() {
        if (last_backpressure_on == -1)
            last_backpressure_on = System.nanoTime();

        if (!backpressure_flag)
            backpressure_flag = true;
    }

    public void backpressure_off() {
        if (last_backpressure_on != -1) {
            backpressure_time += System.nanoTime() - last_backpressure_on;
            last_backpressure_on = -1;
        }
    }

    public void writeToDist(String file) {
        try {
            PrintWriter out = new PrintWriter(new File(file));
            out.println("backpressure_flag = " + backpressure_flag);
            out.println("backpressure_time = " + backpressure_time + " ns");
            out.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
}

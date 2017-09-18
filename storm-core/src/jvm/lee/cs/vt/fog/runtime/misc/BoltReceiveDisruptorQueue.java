package lee.cs.vt.fog.runtime.misc;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.dsl.ProducerType;
import lee.cs.vt.fog.runtime.FogRuntime;
import org.apache.storm.utils.DisruptorQueue;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class BoltReceiveDisruptorQueue extends DisruptorQueue {
    private final Lock lock = FogRuntime.LOCK;
    private final Condition condition = FogRuntime.CONDITION;

    private boolean isSpout = false;

    private boolean isRunning = false;

    public BoltReceiveDisruptorQueue(String queueName,
                                     ProducerType type,
                                     int size,
                                     long readTimeout,
                                     int inputBatchSize,
                                     long flushInterval) {
        super(queueName, type, size, readTimeout, inputBatchSize, flushInterval);
    }

    @Override
    protected void publishDirectSingle(Object obj, boolean block) throws InsufficientCapacityException {
        long at;
        if (block) {
            at = _buffer.next();
        } else {
            at = _buffer.tryNext();
        }
        AtomicReference<Object> m = _buffer.get(at);
        m.set(obj);
        _buffer.publish(at);
        _metrics.notifyArrivals(1);

        if (!isSpout) {
            lock.lock();
            condition.signalAll();
            lock.unlock();
        }
    }

    @Override
    protected void publishDirect(ArrayList<Object> objs, boolean block) throws InsufficientCapacityException {
        int size = objs.size();
        if (size > 0) {
            long end;
            if (block) {
                end = _buffer.next(size);
            } else {
                end = _buffer.tryNext(size);
            }
            long begin = end - (size - 1);
            long at = begin;
            for (Object obj: objs) {
                AtomicReference<Object> m = _buffer.get(at);
                m.set(obj);
                at++;
            }
            _buffer.publish(begin, end);
            _metrics.notifyArrivals(size);

            if (!isSpout) {
                lock.lock();
                condition.signalAll();
                lock.unlock();
            }
        }
    }

    public void setSpout() {
        isSpout = true;
    }

    public void setIsRunning() {
        isRunning = true;
    }

    public void resetIsRunning() {
        isRunning = false;
    }
}


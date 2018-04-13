package lee.cs.vt.fog.runtime.misc;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.dsl.ProducerType;
import lee.cs.vt.fog.runtime.FogRuntime;
import org.apache.storm.utils.DisruptorQueue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;

public class BoltReceiveDisruptorQueue extends DisruptorQueue {
    private final Lock lock = FogRuntime.LOCK;
    private final Condition condition = FogRuntime.CONDITION;

    private boolean isSpout = false;

    private long waitStartTime = -1;
    private long totalWaitTime = 0;
    private long totalTupleConsumed = 0;

    private long emptyStartTime = -1;
    private long totalEmptyTime = 0;

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

        if (FogRuntime.getWaitTime &&
                _metrics.population() == 1) {
            setWaitStartTime();
        }

        if (FogRuntime.getEmptyTime &&
                _metrics.population() == 1) {
            addEmptyTime();
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

            if (FogRuntime.getWaitTime &&
                    _metrics.population() == size) {
                setWaitStartTime();
            }

            if (FogRuntime.getEmptyTime &&
                    _metrics.population() == size) {
                addEmptyTime();
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

    @Override
    protected void consumeBatchToCursor(long cursor, EventHandler<Object> handler) {
        for (long curr = _consumer.get() + 1; curr <= cursor; curr++) {
            try {
                AtomicReference<Object> mo = _buffer.get(curr);
                Object o = mo.getAndSet(null);
                if (o == INTERRUPT) {
                    throw new InterruptedException("Disruptor processing interrupted");
                } else if (o == null) {
                    LOG.error("NULL found in {}:{}", this.getName(), cursor);
                } else {
                    handler.onEvent(o, curr, curr == cursor);

                    if (FogRuntime.getWaitTime) {
                        List list = (List) o;
                        totalTupleConsumed += list.size();
                    }

                    if (_enableBackpressure && _cb != null && (_metrics.writePos() - curr + _overflowCount.get()) <= _lowWaterMark) {
                        try {
                            if (_throttleOn) {
                                _throttleOn = false;
                                _cb.lowWaterMark();
                            }
                        } catch (Exception e) {
                            throw new RuntimeException("Exception during calling lowWaterMark callback!");
                        }
                    }
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        _consumer.set(cursor);
    }

    public void setSpout() {
        isSpout = true;
    }

    public void setWaitStartTime() {
        waitStartTime = System.currentTimeMillis();
    }

    public void addWaitTime() {
        if (waitStartTime == -1) {
            return;
        }
        totalWaitTime += System.currentTimeMillis() - waitStartTime;
        waitStartTime = -1;
    }

    public long getTotalWaitTime() {
        return totalWaitTime;
    }

    public long getTotalTupleConsumed() {
        return totalTupleConsumed;
    }

    public void setEmptyStartTime() {
        emptyStartTime = System.currentTimeMillis();
    }

    private void addEmptyTime() {
        if (emptyStartTime == -1) {
            return;
        }
        totalEmptyTime += System.currentTimeMillis() - emptyStartTime;
        emptyStartTime = -1;
    }

    public long getTotalEmptyTime() {
        return totalEmptyTime;
    }
}


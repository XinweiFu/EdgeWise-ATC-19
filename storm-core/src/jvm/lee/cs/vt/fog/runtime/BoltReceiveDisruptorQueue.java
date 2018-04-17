package lee.cs.vt.fog.runtime;

import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.dsl.ProducerType;
import org.apache.storm.metric.internal.MultiCountStatAndMetric;
import org.apache.storm.utils.DisruptorQueue;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicReference;

public class BoltReceiveDisruptorQueue extends DisruptorQueue {

    private boolean isSpout = false;
    private MultiCountStatAndMetric waitLatencyMetric = null;

    private long waitStartTime = -1;
    private long totalWaitTime = 0;

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

        if (!isSpout && _metrics.population() == 1) {
            setWaitStartTime();
            addEmptyTime();
        }

        AtomicReference<Object> m = _buffer.get(at);
        m.set(obj);
        _buffer.publish(at);
        _metrics.notifyArrivals(1);
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

            if (!isSpout && _metrics.population() == size) {
                setWaitStartTime();
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
        }
    }

    @Override
    protected void consumeBatchToCursor(long cursor, EventHandler<Object> handler) {
        if (!isSpout) {
            addWaitTime();
        }

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

        if (!isSpout) {
            if(_metrics.population() > 0){
                setWaitStartTime();
            } else {
                setEmptyStartTime();
            }
        }
    }

    public void setSpout() {
        isSpout = true;
    }

    public void setWaitLatMetric(MultiCountStatAndMetric waitLatencyMetric) {
        this.waitLatencyMetric = waitLatencyMetric;
    }

    private void setWaitStartTime() {
        waitStartTime = System.currentTimeMillis();
    }

    private void addWaitTime() {
        if (waitStartTime == -1) {
            return;
        }
        long delta = System.currentTimeMillis() - waitStartTime;
        totalWaitTime += delta;
        waitStartTime = -1;

        waitLatencyMetric.incBy("default", delta);
    }

    public long getTotalWaitTime() {
        return totalWaitTime;
    }

    private void setEmptyStartTime() {
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

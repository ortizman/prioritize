package com.executor.priority;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.executor.priority.Importance.TaskPriority.HIGH;
import static com.executor.priority.Importance.TaskPriority.MEDIUM;

public class MultiPriorityBlockingQueue<T extends Runnable & Importance> extends AbstractQueue<T>
        implements BlockingQueue<T> {

    private final BlockingQueue<T> highPriorityQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<T> mediumPriorityQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<T> lowPriorityQueue = new LinkedBlockingQueue<>();

    /**
     * Lock held by take and poll
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Waiting queue for takes
     */
    private final Condition notEmpty = lock.newCondition();

    private NextQueueAlgorithm<T> nextQueueAlgorithm;

    public MultiPriorityBlockingQueue(NextQueueAlgorithm<T> algorithm) {
        super();
        algorithm.init(this);
        this.nextQueueAlgorithm = algorithm;
    }

    public MultiPriorityBlockingQueue() {
        // Creo e inicializo el algoritmo por defecto
        this(new PriorityNextQueueAlgorithm<>());
        //this.setNextQueueAlgorithm(new RoundRobinNextQueueAlgorithm<>());
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiPriorityBlockingQueue.class);

    /**
     * TODO: ver si tiene sentido implementarlo
     *
     * @return
     */
    @Override
    public Iterator<T> iterator() {
        throw new RuntimeException("Aun no implementado");
    }

    @Override
    public int size() {
        return highPriorityQueue.size() + mediumPriorityQueue.size() + lowPriorityQueue.size();
    }

    @Override
    public void put(T task) throws InterruptedException {
        if (HIGH.equals(task.getPriority())) {
            highPriorityQueue.put(task);
        } else if (MEDIUM.equals(task.getPriority())) {
            mediumPriorityQueue.put(task);
        } else {
            lowPriorityQueue.put(task);
        }
    }

    @Override
    public boolean offer(T task) {
        boolean result;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (HIGH.equals(task.getPriority())) {
                result = highPriorityQueue.offer(task);
            } else if (MEDIUM.equals(task.getPriority())) {
                result = mediumPriorityQueue.offer(task);
            } else {
                result = lowPriorityQueue.offer(task);
            }
            notEmpty.signal();
            return result;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean offer(T task, long timeout, TimeUnit timeUnit) throws InterruptedException {
        boolean result;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (HIGH.equals(task.getPriority())) {
                result = highPriorityQueue.offer(task, timeout, timeUnit);
            } else if (MEDIUM.equals(task.getPriority())) {
                result = mediumPriorityQueue.offer(task, timeout, timeUnit);
            } else {
                result = lowPriorityQueue.offer(task, timeout, timeUnit);
            }
            notEmpty.signal();
            return result;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retorna una tarea de las colas de prioridades de acuerdo con la politica seteada en {@link #nextQueueAlgorithm}
     * Extra Ref: {@link LinkedBlockingQueue#take()}
     *
     * @return una tarea
     * @throws InterruptedException
     */
    @Override
    public T take() throws InterruptedException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            BlockingQueue<T> nextQueue = nextQueueAlgorithm.getNextQueue();
            try {
                while (nextQueue == null) {
                    notEmpty.await();
                    nextQueue = nextQueueAlgorithm.getNextQueue();
                }
            } catch (InterruptedException ie) {
                notEmpty.signal();
                throw ie;
            }
            return nextQueue.take();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retorna una tarea de las colas de prioridades de acuerdo con la politica seteada en {@link #nextQueueAlgorithm}
     * Extra Ref: {@link LinkedBlockingQueue#poll()}
     *
     * @return una tarea
     */
    @Override
    public T poll() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            BlockingQueue<T> nextQueue = nextQueueAlgorithm.getNextQueue();
            if (nextQueue == null) {
                return null;
            }
            return nextQueue.poll();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Retorna una tarea de las colas de prioridades de acuerdo con la politica seteada en {@link #nextQueueAlgorithm}
     * Extra Ref: {@link LinkedBlockingQueue#poll(long, TimeUnit)}
     *
     * @param timeout
     * @param timeUnit
     * @return
     * @throws InterruptedException
     */
    @Override
    public T poll(long timeout, TimeUnit timeUnit) throws InterruptedException {
        long nanos = timeUnit.toNanos(timeout);
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            BlockingQueue<T> nextQueue = nextQueueAlgorithm.getNextQueue();
            while (nextQueue == null) {
                if (nanos <= 0L)
                    return null;
                nanos = this.notEmpty.awaitNanos(nanos);
                nextQueue = nextQueueAlgorithm.getNextQueue();
            }

            return nextQueue.poll();

        } finally {
            lock.unlock();
        }
    }

    @Override
    public T peek() {
        BlockingQueue<T> nextQueue = nextQueueAlgorithm.getNextQueue();
        if (nextQueue == null) {
            return null;
        }
        return nextQueue.peek();
    }

    /**
     * No se maneja limites para la capacidad de la cola
     *
     * @return Integer.MAX_VALUE
     */
    @Override
    public int remainingCapacity() {
        return Integer.MAX_VALUE;
    }

    /**
     * Ningun elemento es drenado
     */
    @Override
    public int drainTo(Collection<? super T> collection) {
        return 0;
    }

    /**
     * Ningun elemento es drenado
     */
    @Override
    public int drainTo(Collection<? super T> collection, int maxElements) {
        return 0;
    }

    BlockingQueue<T> getHighPriorityQueue() {
        return highPriorityQueue;
    }

    BlockingQueue<T> getMediumPriorityQueue() {
        return mediumPriorityQueue;
    }

    BlockingQueue<T> getLowPriorityQueue() {
        return lowPriorityQueue;
    }

    public NextQueueAlgorithm<T> getNextQueueAlgorithm() {
        return nextQueueAlgorithm;
    }

    public void setNextQueueAlgorithm(NextQueueAlgorithm<T> nextQueueAlgorithm) {
        nextQueueAlgorithm.init(this);
        this.nextQueueAlgorithm = nextQueueAlgorithm;
    }
}
package com.executor.priority;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.ReentrantLock;

public class RoundRobinNextQueueAlgorithm<T extends Runnable & Importance> implements NextQueueAlgorithm<T> {

    private final ReentrantLock lock = new ReentrantLock();

    private final Queue<BlockingQueue<T>> linkedQueue = new ArrayBlockingQueue<>(3);

    /**
     * Inicializa el algoritmo
     * Queue<T> high queue de prioridad alta
     * Queue<T> medium queue de prioridad media
     * Queue<T> low queue de prioridad baja
     * @param multiPriorityBlockingQueue
     */
    @Override
    public void init(MultiPriorityBlockingQueue<T> multiPriorityBlockingQueue) {
        linkedQueue.add(multiPriorityBlockingQueue.getHighPriorityQueue());
        linkedQueue.add(multiPriorityBlockingQueue.getMediumPriorityQueue());
        linkedQueue.add(multiPriorityBlockingQueue.getLowPriorityQueue());
    }

    @Override
    public BlockingQueue<T> getNextQueue() {
        lock.lock();
        try {
            BlockingQueue<T> nextQueue = linkedQueue.poll();
            linkedQueue.add(nextQueue);
            if (nextQueue.isEmpty()) {
                return null;
            } else {
                return nextQueue;
            }
        } finally {
            lock.unlock();
        }

    }
}

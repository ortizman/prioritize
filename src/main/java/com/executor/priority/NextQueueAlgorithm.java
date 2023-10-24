package com.executor.priority;

import java.util.concurrent.BlockingQueue;

/**
 * Esta interface abstrae el algoritmo que determina cual es la siguiente tarea que debe ejecutarse.
 *
 * @param <T>
 */
interface NextQueueAlgorithm<T extends Runnable & Importance> {

    void init(MultiPriorityBlockingQueue<T> multiPriorityBlockingQueue);

    /**
     * Should return a queue based on some selection criteria and current
     * state of the queues.
     *
     * @return the queue
     */
    BlockingQueue<T> getNextQueue();
}

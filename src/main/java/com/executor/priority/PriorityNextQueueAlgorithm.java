package com.executor.priority;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import static com.executor.priority.Constants.*;

public class PriorityNextQueueAlgorithm<T extends Runnable & Importance> implements NextQueueAlgorithm<T> {

    private BlockingQueue<T> high;
    private BlockingQueue<T> medium;
    private BlockingQueue<T> low;

    private final AtomicInteger highTaskTaken = new AtomicInteger(6);
    private final AtomicInteger mediumTaskTaken = new AtomicInteger(3);
    private final AtomicInteger lowTaskTaken = new AtomicInteger(1);
    private final AtomicInteger totalTaskTaken = new AtomicInteger(10);

    private final ReentrantLock lock = new ReentrantLock();

    private static final Logger LOGGER = LoggerFactory.getLogger(PriorityNextQueueAlgorithm.class);

    /**
     * Inicializa el algoritmo
     * Queue<T> high queue de prioridad alta
     * Queue<T> medium queue de prioridad media
     * Queue<T> low queue de prioridad baja
     *
     * @param multiPriorityBlockingQueue
     */
    public void init(MultiPriorityBlockingQueue<T> multiPriorityBlockingQueue) {

        // Timer que imprime cada X segundos el estado de la cola de prioridades
        TimerTask timerTask = new TimerTask() {
            public void run() {
                LOGGER.trace(
                        "\nLow Queue size: {}, \nMEDIUM Queue size: {}, \nHIGH Queue size: {}. \nLow Counter: {}. \nMedium Counter: {}. \nHIGH Counter: {}.\nTotal Counter: {}.",
                        low.size(), medium.size(), high.size(),
                        lowTaskTaken.get(), mediumTaskTaken.get(), highTaskTaken.get(),
                        totalTaskTaken.get());
            }
        };
        Timer timer = new Timer();
        // Dentro de 0 milisegundos avisame cada 3000 milisegundos
        timer.scheduleAtFixedRate(timerTask, 0, 30 * 1000);

        this.high = multiPriorityBlockingQueue.getHighPriorityQueue();
        this.medium = multiPriorityBlockingQueue.getMediumPriorityQueue();
        this.low = multiPriorityBlockingQueue.getLowPriorityQueue();
    }

    @Override
    public BlockingQueue<T> getNextQueue() {

        lock.lock();
        try {
            double totalTaskTaken = this.totalTaskTaken.get();
            if (totalTaskTaken < 0 || totalTaskTaken > (Integer.MAX_VALUE - 100000)) {
                // reseteo los contadores cuando me excedo del limite de Integer.MAX_VALUE
                resetCounters();
            }
            Double highScore = (totalTaskTaken / highTaskTaken.get()) * 0.6;
            Double mediumScore = (totalTaskTaken / mediumTaskTaken.get()) * 0.3;
            Double lowScore = (totalTaskTaken / lowTaskTaken.get()) * 0.1;

            // El score mas grande indica la cola de la cual debo tomar la siguiente tarea
            return getQueueByScore(highScore, mediumScore, lowScore);
        } finally {
            lock.unlock();
        }
    }

    private BlockingQueue<T> getQueueByScore(Double highScore, Double mediumScore, Double lowScore) {

        // Caso base: todas las colas estan vacias
        // entonces, retorno null
        if (highScore == -1d && mediumScore == -1d && lowScore == -1d) {
            return null;
        }

        double scoreMax = Math.max(Math.max(highScore, mediumScore), lowScore);
        if (scoreMax == highScore) {
            highTaskTaken.incrementAndGet();
            totalTaskTaken.incrementAndGet();
            if (high.isEmpty()) {
                return getQueueByScore(-1d, mediumScore, lowScore);
            }
            return high;
        }

        if (scoreMax == mediumScore) {
            mediumTaskTaken.incrementAndGet();
            totalTaskTaken.incrementAndGet();
            if (medium.isEmpty()) {
                return getQueueByScore(highScore, -1d, lowScore);
            }
            return medium;
        }

        lowTaskTaken.incrementAndGet();
        totalTaskTaken.incrementAndGet();

        if (low.isEmpty()) {
            return getQueueByScore(highScore, mediumScore, -1d);
        }

        return low;
    }

    private void resetCounters() {
        totalTaskTaken.set(10);
        lowTaskTaken.set(1);
        mediumTaskTaken.set(3);
        highTaskTaken.set(6);
    }
}

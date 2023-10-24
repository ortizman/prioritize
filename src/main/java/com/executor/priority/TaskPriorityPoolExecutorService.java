package com.executor.priority;

import com.executor.priority.Importance.TaskPriority;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * Ejecuta tareas con tres diferentes niveles de prioridad (ALTA, MEDIA, BAJA).
 * Todas las tareas eventualmente toman tiempo de CPU, donde:
 * 1. La tarea de prioridad ALTA tendrá mayor tiempo de procesamiento que la de prioridad MEDIA
 * 2. La tarea de prioridad MEDIA tendrá mayor tiempo de procesamiento que la de prioridad BAJA
 */
public class TaskPriorityPoolExecutorService implements ExecutorService {
    // unica instancia que se inicializa de forma lazy
    private static volatile TaskPriorityPoolExecutorService INSTANCE = null;

    private ThreadPoolExecutor executor;

    /**
     * Cantidad de hilos en el pool
     */
    private int core = Integer.parseInt(System.getProperty("executor.threads.core", "20"));

    /**
     * Numero maximo de threads en el pool
     */
    private int max = Integer.parseInt(System.getProperty("executor.threads.max", "60"));

    /**
     * tiempo de inactividad de un thread antes de eliminarlo del pool
     */
    private int keepAlive = Integer.parseInt(System.getProperty("executor.threads.keepAlive", "5"));

    /**
     * Cola de prioridades fairness
     */
    private MultiPriorityBlockingQueue<? extends Runnable> queue;

    /**
     * Weather executor is initializer
     */
    private boolean initialized;

    private static final Logger LOGGER = LoggerFactory.getLogger(TaskPriorityPoolExecutorService.class);

    /**
     * Genera una unica instancia y retorna siempre la misma en las posteriores invocaciones.
     *
     * @return una singleton de TaskPriorityPoolExecutorService
     */
    public static TaskPriorityPoolExecutorService getInstance() {
        if (INSTANCE == null) { // Check 1
            synchronized (TaskPriorityPoolExecutorService.class) {
                if (INSTANCE == null) { // Check 2
                    INSTANCE = new TaskPriorityPoolExecutorService() {
                        @Override
                        public void execute(Runnable runnable) {
                            // Transfiere el contexto MDC para logging de txid en hilo de ejecución
                            final Map<String, String> mdc = MDC.getCopyOfContextMap();
                            super.execute(() -> {
                                MDC.setContextMap(mdc);
                                try {
                                    runnable.run();
                                } finally {
                                    MDC.clear();
                                }
                            });
                        }
                    };
                }
            }
        }
        return INSTANCE;
    }

    private TaskPriorityPoolExecutorService() {
        init();
    }

    /**
     * Initialize the executor by using the properties. Create the queues
     * and ThreadPool executor.
     */
    private void init() {

        this.queue = new MultiPriorityBlockingQueue<>();
        executor = new ThreadPoolExecutor(
                core,
                max,
                keepAlive,
                TimeUnit.SECONDS,
                (BlockingQueue<Runnable>) queue,
                new CustomThreadFactory(new ThreadGroup("task-priority-executor"), "priority-worker"));

        initialized = true;

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Started the thread pool executor with threads, " +
                    "core = " + core + " max = " + max +
                    ", keep-alive = " + keepAlive);
        }
    }

    /**
     * Execute a given task with the priority specified. If the task throws an exception,
     * it will be captured and logged to prevent the threads from dying.
     *
     * @param task     task to be executed
     * @param priority priority of the task
     */
    private void execute(final Runnable task, TaskPriority priority) {
        if (!initialized) {
            throw new IllegalStateException("Executor is not initialized");
        }
        // Wrapper de la tarea. Ejecuta dentro de un bloque try-catch
        // para evitar que un error inesperado elimine el thread
        Worker w = new Worker(task, priority);

        executor.execute(w);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(Runnable runnable) {
        this.execute(runnable, TaskPriority.LOW);
    }

    /**
     * Eventualmente ejecuta una nueva tarea con la prioridad especificada en el parametro taskPriority
     *
     * @param taskPriority valores posibles {"HIGH", "MEDIUM", "LOW"}.
     * @param runnable the runnable task
     */
    public void execute(Runnable runnable, String taskPriority) {
        TaskPriority priority = TaskPriority.fromString(taskPriority);
        this.execute(runnable, priority);
    }

    public void setNextQueueAlgorithm(NextQueueAlgorithm nextQueueAlgorithm) {
        this.queue.setNextQueueAlgorithm(nextQueueAlgorithm);
    }

    public NextQueueAlgorithm getNextQueueAlgorithm() {
        return this.queue.getNextQueueAlgorithm();
    }

    /**
     * Set the queue.
     *
     * @param queue queue used for handling the priorities
     */
    public void setQueue(MultiPriorityBlockingQueue<? extends Runnable> queue) {
        this.queue = queue;
    }

    /**
     * Get the queue.
     *
     * @return queue used for handling multiple priorities
     */
    public MultiPriorityBlockingQueue<? extends Runnable> getQueue() {
        return queue;
    }

    /**
     * Get the core number of threads
     *
     * @return core number of threads
     */
    public int getCore() {
        return core;
    }

    /**
     * Get the max threads
     *
     * @return max thread
     */
    public int getMax() {
        return max;
    }

    /**
     * Get the keep alive time for threads
     *
     * @return keep alive time for threads
     */
    public int getKeepAlive() {
        return keepAlive;
    }

    /**
     * Set the core number of threads
     *
     * @param core core number of threads
     */
    public void setCore(int core) {
        this.core = core;
    }

    /**
     * Set the max number of threads
     *
     * @param max max threads
     */
    public void setMax(int max) {
        this.max = max;
    }

    /**
     * Set the keep alive time for threads
     *
     * @param keepAlive keep alive threads
     */
    public void setKeepAlive(int keepAlive) {
        this.keepAlive = keepAlive;
    }

    /**
     * Clase privada para ejecutar tareas. Es un wrapper de {@link Runnable} para prevenir
     * que los hilos mueran ante errores inesperados. Además, implementa la interfaz
     * {@link Importance}
     */
    private static class Worker implements Runnable, Importance {
        private Runnable runnable;
        private TaskPriority priority;

        private Worker(Runnable runnable, TaskPriority priority) {
            this.priority = priority;
            this.runnable = runnable;
        }

        public void run() {
            try {
                runnable.run();
            } catch (Exception e) {
                LOGGER.error("Unhandled exception", e);
            }
        }

        public TaskPriority getPriority() {
            return priority;
        }
    }

    @Override
    public void shutdown() {
        this.executor.shutdown();
    }

    @Override
    public List<Runnable> shutdownNow() {
        return this.executor.shutdownNow();
    }

    @Override
    public boolean isShutdown() {
        return this.executor.isShutdown();
    }

    @Override
    public boolean isTerminated() {
        return this.executor.isTerminated();
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return this.executor.awaitTermination(timeout, timeUnit);
    }

    @Override
    public <T> Future<T> submit(Callable<T> callable) {
        return this.executor.submit(callable);
    }

    @Override
    public <T> Future<T> submit(Runnable runnable, T t) {
        return this.executor.submit(runnable, t);
    }

    @Override
    public Future<?> submit(Runnable runnable) {
        return this.executor.submit(runnable);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection) throws InterruptedException {
        return this.executor.invokeAll(collection);
    }

    @Override
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> collection, long timeout, TimeUnit timeUnit) throws InterruptedException {
        return this.executor.invokeAll(collection, timeout, timeUnit);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> collection) throws InterruptedException, ExecutionException {
        return this.executor.invokeAny(collection);
    }

    @Override
    public <T> T invokeAny(Collection<? extends Callable<T>> collection, long timeout, TimeUnit timeUnit) throws InterruptedException, ExecutionException, TimeoutException {
        return this.executor.invokeAny(collection, timeout, timeUnit);
    }
}

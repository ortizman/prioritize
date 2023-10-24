package com.executor.priority;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;

public class TaskPriorityPoolExecutorServiceTest {

    private final static Logger LOGGER = LoggerFactory.getLogger(TaskPriorityPoolExecutorServiceTest.class);

    private static final long sleep = 10000L;

    private TaskPriorityPoolExecutorService executorService;
    private ConcurrentLinkedQueue<String> results;

    @BeforeEach
    void setUp() {
        results = new ConcurrentLinkedQueue<>();
        this.executorService = TaskPriorityPoolExecutorService.getInstance();
    }

    @Test
    public void testExecutorWithPriorityNextQueueAlgorithm_equalsTaskNumber() {
        int effort = 10_000;
        int cantTask = 1000;

        // especifico el algoritmo (strategy) que se usa para balancear la ejecucion de tareas.
        this.executorService.setNextQueueAlgorithm(new PriorityNextQueueAlgorithm<>());

        // inicializa la cola de trabajo con la misma cantidad de tareas de prioridad HIGH, MEDIUM y LOW
        Set<String> priorities = Set.of("LOW", "MEDIUM", "HIGH");
        loadTasks(effort, cantTask, priorities);

        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            Assertions.fail("Unexpected error");
        }

        // analizar el resultado de la cola
        Assertions.assertEquals(results.size(), cantTask * priorities.size(), "No se completaron todas las tareas");

        int position = 1;
        double highSum = 0, mediumSum = 0, lowSum = 0;
        for (String taskPriority : results) {
            if ("HIGH".equalsIgnoreCase(taskPriority)) {
                highSum += position;
            } else if ("MEDIUM".equalsIgnoreCase(taskPriority)) {
                mediumSum += position;
            } else if ("LOW".equalsIgnoreCase(taskPriority)) {
                lowSum += position;
            } else {
                throw new IllegalStateException("No se puede interpretar la prioridad");
            }
            position++;
        }

        double total = lowSum + mediumSum + highSum;

        double lowProp = total / lowSum;
        Assertions.assertTrue(1.0 <= lowProp && lowProp <= 2.01,
                "Las tareas de baja prioridad en forma desproporcional. Resultado: " + lowProp);

        double mediumProp = total / mediumSum;
        Assertions.assertTrue(3.0 <= mediumProp && mediumProp <= 4.01,
                "Las tareas de baja prioridad en forma desproporcional. Resultado: " + mediumProp);

        double highProp = total / highSum;
        Assertions.assertTrue(5.0 <= highProp && highProp <= 6.01,
                "Las tareas de baja prioridad en forma desproporcional. Resultado: " + highProp);

    }

    @Test
    public void testExecutorWithPriorityNextQueueAlgorithm_withoutLowTasks() {
        int effort = 10_000;
        int cantTask = 1000;

        // especifico el algoritmo (strategy) que se usa para balancear la ejecucion de tareas.
        this.executorService.setNextQueueAlgorithm(new PriorityNextQueueAlgorithm<>());

        // inicializa la cola de trabajo con la misma cantidad de tareas de prioridad HIGH, MEDIUM y LOW
        Set<String> priorities = Set.of("MEDIUM", "HIGH");
        loadTasks(effort, cantTask, priorities);

        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            Assertions.fail("Unexpected error");
        }

        // analizar el resultado de la cola
        Assertions.assertEquals(results.size(), cantTask * priorities.size(), "No se completaron todas las tareas");

        int position = 1;
        double highSum = 0, mediumSum = 0, lowSum = 0;
        for (String taskPriority : results) {
            if ("HIGH".equalsIgnoreCase(taskPriority)) {
                highSum += position;
            } else if ("MEDIUM".equalsIgnoreCase(taskPriority)) {
                mediumSum += position;
            } else if ("LOW".equalsIgnoreCase(taskPriority)) {
                lowSum += position;
            } else {
                throw new IllegalStateException("No se puede interpretar la prioridad");
            }
            position++;
        }

        double total = lowSum + mediumSum + highSum;

        Assertions.assertEquals(0, lowSum, "No deberia haber ninguna tarea de baja prioridad en los resultados");

        double mediumProp = 100 - (mediumSum * 100) / total;
        Assertions.assertTrue(30 <= mediumProp && mediumProp <= 40,
                "Las tareas de prioridad media se ejecutaron en forma desproporcional. Resultado: " + mediumProp);

        double highProp = 100 - (highSum * 100) / total;
        Assertions.assertTrue(60 <= highProp && highProp <= 70,
                "Las tareas de prioridad alta se ejecutaron en forma desproporcional. Resultado: " + highProp);

    }

    @Test
    public void testExecutorWithPriorityNextQueueAlgorithm_withoutMediumTasks() {
        int effort = 10_000;
        int cantTask = 1000;

        // especifico el algoritmo (strategy) que se usa para balancear la ejecucion de tareas.
        this.executorService.setNextQueueAlgorithm(new PriorityNextQueueAlgorithm<>());

        // inicializa la cola de trabajo con la misma cantidad de tareas de prioridad HIGH, MEDIUM y LOW
        Set<String> priorities = Set.of("LOW", "HIGH");
        loadTasks(effort, cantTask, priorities);

        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            Assertions.fail("Unexpected error");
        }

        // analizar el resultado de la cola
        Assertions.assertEquals(results.size(), cantTask * priorities.size(), "No se completaron todas las tareas");

        int position = 1;
        double highSum = 0, mediumSum = 0, lowSum = 0;
        for (String taskPriority : results) {
            if ("HIGH".equalsIgnoreCase(taskPriority)) {
                highSum += position;
            } else if ("MEDIUM".equalsIgnoreCase(taskPriority)) {
                mediumSum += position;
            } else if ("LOW".equalsIgnoreCase(taskPriority)) {
                lowSum += position;
            } else {
                throw new IllegalStateException("No se puede interpretar la prioridad");
            }
            position++;
        }

        double total = lowSum + mediumSum + highSum;

        Assertions.assertEquals(0, mediumSum, "No deberia haber ninguna tarea de baja prioridad en los resultados");

        double mediumProp = 100 - (lowSum * 100) / total;
        Assertions.assertTrue(25 <= mediumProp && mediumProp <= 35,
                "Las tareas de prioridad media se ejecutaron en forma desproporcional. Resultado: " + mediumProp);

        double highProp = 100 - (highSum * 100) / total;
        Assertions.assertTrue(65 <= highProp && highProp <= 75,
                "Las tareas de prioridad alta se ejecutaron en forma desproporcional. Resultado: " + highProp);

    }

    @Test
    public void testExecutorWithRoundRobinNextQueueAlgorithm() {
        int effort = 10_000;
        int cantTask = 1000;

        // especifico el algoritmo (strategy) que se usa para balancear la ejecucion de tareas.
        this.executorService.setNextQueueAlgorithm(new RoundRobinNextQueueAlgorithm<>());

        // inicializa la cola de trabajo con la misma cantidad de tareas de prioridad HIGH, MEDIUM y LOW
        Set<String> priorities = Set.of("LOW", "MEDIUM", "HIGH");
        loadTasks(effort, cantTask, priorities);

        try {
            Thread.sleep(sleep);
        } catch (InterruptedException e) {
            Assertions.fail("Unexpected error");
        }

        // analizar el resultado de la cola
        Assertions.assertEquals(results.size(), cantTask * priorities.size(), "No se completaron todas las tareas");

        int position = 1;
        double highSum = 0, mediumSum = 0, lowSum = 0;
        for (String taskPriority : results) {
            if ("HIGH".equalsIgnoreCase(taskPriority)) {
                highSum += position;
            } else if ("MEDIUM".equalsIgnoreCase(taskPriority)) {
                mediumSum += position;
            } else if ("LOW".equalsIgnoreCase(taskPriority)) {
                lowSum += position;
            } else {
                throw new IllegalStateException("No se puede interpretar la prioridad");
            }
            position++;
        }

        double lowerBound = 0.99;
        double upperBound = 1.01;

        Assertions.assertTrue( lowerBound <= (lowSum / mediumSum) && (lowSum / mediumSum) < upperBound,
                "Existe una desproporcion entre la cantidad de tareas de prioridad baja y de media " +
                        "que se ejecutaron. Proporcion: " + (lowSum / mediumSum) + ". " +
                        "El rango admisible es lowerBound: " + lowerBound + ", upperBound: " + upperBound);
        Assertions.assertTrue(lowerBound <= (mediumSum / highSum) && (mediumSum / highSum) < upperBound,
                "Existe una desproporcion entre la cantidad de tareas de prioridad media y de alta " +
                        "que se ejecutaron" + (mediumSum / highSum) + ". " +
                        "El rango admisible es lowerBound: " + lowerBound + ", upperBound: " + upperBound);

    }

    private int loadTasks(int effort, int cantTask, Set<String> priorities) {
        for (int i = 0; i < cantTask; i++) {
            priorities.forEach(s -> addTask(s, effort));
        }
        return cantTask;
    }

    private void addTask(String priority, int effort) {
        this.executorService.execute(() -> {
            long cont = 0;
            for (long j = 0; j < effort; j++) {
                cont += j * new Random().nextInt();
            }
            LOGGER.info(priority + cont);
            results.add(priority);
        }, priority);
    }
}
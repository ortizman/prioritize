package com.executor.priority;

/**
 * Esta interface determina la prioridad de la tarea
 */
public interface Importance {

    enum TaskPriority {
        HIGH, MEDIUM, LOW;

        public static TaskPriority fromString(String label) {
            for (TaskPriority priority : TaskPriority.values()) {
                if (priority.toString().equalsIgnoreCase(label)) {
                    return priority;
                }
            }
            return LOW;
        }
    }

    /**
     * Obtiene la prioridad
     *
     * @return priority
     */
    TaskPriority getPriority();

}
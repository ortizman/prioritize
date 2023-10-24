package com.executor.priority;

final class Constants {

    private Constants() {
        // hidden constructor
    }

    static final String QUEUE_SIZE_METRIC_NAME = "executor.queue.size";
    static final String PRIORITY_TAG_NAME = "priority";
    static final String TAG_NAME = "name";
    static final String TAG_NAME_VALUE = "priority-worker";
    static final String QUEUE_TASK_TAKEN_METRIC_NAME = "executor.task.taken";
    static final String TASK_PRIORITY_TAG_NAME = "priority";

}

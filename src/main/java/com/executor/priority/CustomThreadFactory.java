package com.executor.priority;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Una implementacion de java.util.concurrent.ThreadFactory
 * Crea los threads con un prefijo indicado
 */
public class CustomThreadFactory implements ThreadFactory {

    final ThreadGroup group;
    final AtomicInteger count;
    final String namePrefix;

    public CustomThreadFactory(final ThreadGroup group, final String namePrefix) {
        super();
        this.count = new AtomicInteger(1);
        this.group = group;
        this.namePrefix = namePrefix;
    }

    public Thread newThread(final Runnable runnable) {
        String name = this.namePrefix + '-' + this.count.getAndIncrement();
        Thread t = new Thread(group, runnable, name, 0);
        t.setDaemon(false);
        t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }
}

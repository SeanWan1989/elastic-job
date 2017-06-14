/*
 * Copyright 1999-2015 dangdang.com.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

package com.dangdang.ddframe.job.cloud.executor;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.AbstractScheduledService;
import lombok.RequiredArgsConstructor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 作业健康检查.
 * 
 * @author gaohongtao
 */
class TaskHealthChecker {
    
    private final AtomicLong version = new AtomicLong(System.currentTimeMillis());
    
    private final Set<String> uncheckedTaskIDs = Sets.newConcurrentHashSet();
    
    private final BlockingQueue<String> unhealthyTaskIDs = new LinkedBlockingQueue<>();
    
    private final ExecutorDriver driver;
    
    private final int timeout;
    
    private final int maxTimeouts;
    
    TaskHealthChecker(final ExecutorDriver driver, final int timeout, final int maxTimeouts) {
        this.driver = driver;
        this.timeout = timeout < 1 ? 30 : timeout;
        this.maxTimeouts = maxTimeouts < 1 ? 5 : timeout;
        new HealthCheckerService().startAsync();
    }
    
    private void check() {
        sendRequest();
        processUnhealthyTasks();
    }
    
    private void sendRequest() {
        uncheckedTaskIDs.clear();
        uncheckedTaskIDs.addAll(DaemonTaskScheduler.getAllRunningTaskIDs());
        for (int i = 0; i < maxTimeouts && !uncheckedTaskIDs.isEmpty(); i++) {
            for (String each : uncheckedTaskIDs) {
                driver.sendFrameworkMessage(new Message("HEALTH_CHECK", version.get(), each).marshall());
            }
            try {
                wait(timeout * 1000);
            } catch (final InterruptedException ignored) {
            }
        }
    }
    
    private void processUnhealthyTasks() {
        version.set(System.currentTimeMillis());
        Set<String> retainUnhealthyTaskIDs = new HashSet<>(unhealthyTaskIDs.size() + uncheckedTaskIDs.size());
        unhealthyTaskIDs.drainTo(retainUnhealthyTaskIDs);
        retainUnhealthyTaskIDs.addAll(uncheckedTaskIDs);
        for (String each : retainUnhealthyTaskIDs) {
            DaemonTaskScheduler.shutdown(Protos.TaskID.newBuilder().setValue(each).build());
            driver.sendStatusUpdate(Protos.TaskStatus.newBuilder().setTaskId(Protos.TaskID.newBuilder().setValue(each)).setState(Protos.TaskState.TASK_UNKNOWN).setMessage("HEALTH_CHECK").build());
        }
    }
    
    void receive(final byte[] bytes) {
        Message message = new Message(bytes);
        if (message.valid(version.get()) && uncheckedTaskIDs.remove(message.taskID) && "UNHEALTHY".equals(message.command)) {
            unhealthyTaskIDs.offer(message.taskID);
        }
    }
    
    private class HealthCheckerService extends AbstractScheduledService {
        
        @Override
        protected void runOneIteration() throws Exception {
            try {
                check();
                //CHECKSTYLE:OFF
            } catch (final Throwable throwable) {
                //CHECKSTYLE:ON
                driver.sendFrameworkMessage(Throwables.getStackTraceAsString(throwable).getBytes());
            }
        }
    
        @Override
        protected Scheduler scheduler() {
            return Scheduler.newFixedDelaySchedule(60, 1, TimeUnit.SECONDS);
        }
    
        @Override
        protected ScheduledExecutorService executor() {
            return super.executor();
        }
    }
    
    @RequiredArgsConstructor
    private static class Message implements Serializable {
        
        private static final Splitter COMMAND_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings();
        
        private final String command;
        
        private final long version;
        
        private final String taskID;
    
        Message(final byte[] message) {
            Iterator<String> iterator = COMMAND_SPLITTER.split(new String(message)).iterator();
            if (iterator.hasNext()) {
                command = iterator.next();
            } else {
                command = null;
            }
            if (iterator.hasNext()) {
                version = Long.valueOf(iterator.next());
            } else {
                version = 0;
            }
            if (iterator.hasNext()) {
                taskID = iterator.next();
            } else {
                taskID = null;
            }
        }
        
        boolean valid(final long latestVersion) {
            return ("HEALTH".equals(command) || "UNHEALTH".equals(command)) && version >= latestVersion;
        }
        
        byte[] marshall() {
            return Joiner.on(",").useForNull("nil").join(command, version, taskID).getBytes();
        }
    } 
    
}

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

package com.dangdang.ddframe.job.cloud.scheduler.mesos;

import com.dangdang.ddframe.job.context.TaskContext;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.mesos.Protos;
import org.apache.mesos.SchedulerDriver;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 任务健康检查.
 * 
 * @author gaohongtao
 */
@RequiredArgsConstructor
@Slf4j
public final class TaskHealthChecker {
    
    private final FacadeService facadeService;
    
    private final AtomicLong healthyTaskNumbers = new AtomicLong();
    
    private final AtomicLong unhealthyTaskNumbers = new AtomicLong();
    
    private final AtomicLong lastTime = new AtomicLong(System.currentTimeMillis());
    
    void process(final SchedulerDriver schedulerDriver, final Protos.ExecutorID executorID, final Protos.SlaveID slaveID, final byte[] bytes) {
        final String message = new String(bytes);
        Iterator<String> iterator = Splitter.on(",").trimResults().omitEmptyStrings().split(message).iterator();
        if (!"HEALTH_CHECK".equals(iterator.next())) {
            return;
        }
        if (!iterator.hasNext()) {
            log.warn("error message {}", message);
            return;
        }
        String version = iterator.next();
        if (!iterator.hasNext()) {
            log.warn("error message {}", message);
            return;
        }
        String result;
        TaskContext taskContext = TaskContext.from(iterator.next());
        if (facadeService.isRunning(taskContext)) {
            result = "HEALTH";
            healthyTaskNumbers.incrementAndGet();
        } else {
            result = "UNHEALTH";
            log.error("unhealthy task {} ", taskContext.getId());
            unhealthyTaskNumbers.incrementAndGet();
        }
        schedulerDriver.sendFrameworkMessage(executorID, slaveID, Joiner.on(",").join(result, version, taskContext.getId()).getBytes());
        metric();
    }
    
    private void metric() {
        if (lastTime.get() + 60 * 1000 > System.currentTimeMillis()) {
            return;
        }
        lastTime.set(System.currentTimeMillis());
        log.info("{}: Health:{} Unhealth:{}", this.getClass().getCanonicalName(), healthyTaskNumbers.getAndSet(0), unhealthyTaskNumbers.getAndSet(0));
    }
    
}

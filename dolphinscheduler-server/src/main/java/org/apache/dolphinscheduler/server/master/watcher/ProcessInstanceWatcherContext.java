package org.apache.dolphinscheduler.server.master.watcher;

import org.apache.dolphinscheduler.dao.entity.ProcessInstance;

/**
 * ProcessInstanceWatcherContext
 *
 * @author liliangshan
 * @date 2024-01-07
 */
public class ProcessInstanceWatcherContext {

    private ProcessInstance processInstance;
    private long timeMs;

    public ProcessInstanceWatcherContext(ProcessInstance processInstance, long timeMs) {
        this.processInstance = processInstance;
        this.timeMs = timeMs;
    }

    public ProcessInstance getProcessInstance() {
        return processInstance;
    }

    public void setProcessInstance(ProcessInstance processInstance) {
        this.processInstance = processInstance;
    }

    public long getTimeMs() {
        return timeMs;
    }

    public void setTimeMs(long timeMs) {
        this.timeMs = timeMs;
    }
}

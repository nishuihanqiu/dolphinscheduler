/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.runner.task;

import org.apache.dolphinscheduler.common.enums.ExecutionStatus;
import org.apache.dolphinscheduler.common.enums.TaskTimeoutStrategy;
import org.apache.dolphinscheduler.common.enums.TaskType;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskDefinition;
import org.apache.dolphinscheduler.remote.command.StateEventChangeCommand;
import org.apache.dolphinscheduler.remote.processor.StateEventCallbackService;
import org.apache.dolphinscheduler.server.utils.LogUtils;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;

import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 *
 */
public class SubTaskProcessor extends BaseTaskProcessor {

    private TaskDefinition taskDefinition;
    private static final long RUNNING_INTERVAL_TIME_MILLS = 1000 * 20;
    private long lastRunningTime = 0L;
    private final AtomicBoolean finished = new AtomicBoolean(false);

    private StateEventCallbackService stateEventCallbackService = SpringApplicationContext.getBean(StateEventCallbackService.class);

    @Override
    public boolean submitTask() {
        this.taskInstance = processService.submitTask(taskInstance, maxRetryTimes, commitInterval);
        if (this.taskInstance == null) {
            return false;
        }

        taskDefinition = processService.findTaskDefinition(
                taskInstance.getTaskCode(), taskInstance.getTaskDefinitionVersion()
        );

        setTaskExecutionLogger();
        initLogPath();
        updateTaskInstanceToRunning();
        logger.info("sub process task start");
        logger.info("sub task processor running.");
        return true;
    }

    private void initLogPath() {
        taskInstance.setLogPath(LogUtils.getTaskLogPath(processInstance.getProcessDefinitionCode(),
                processInstance.getProcessDefinitionVersion(),
                taskInstance.getProcessInstanceId(),
                taskInstance.getId()));
    }

    private void updateTaskInstanceToRunning() {
        taskInstance.setHost(NetUtils.getAddr(masterConfig.getListenPort()));
        taskInstance.setState(ExecutionStatus.RUNNING_EXECUTION);
        taskInstance.setSubmitTime(null);
        taskInstance.setStartTime(new Date());
        processService.updateTaskInstance(taskInstance);
    }

    @Override
    public ExecutionStatus taskState() {
        return this.taskInstance.getState();
    }

    @Override
    public boolean runTask() {
        try {
            if (!this.runningChecked()) {
                return true;
            }
            if (!finished.get()) {
                endTask();
                return true;
            }
            logger.info("sub task processor run task end");
        } catch (Exception e) {
            logger.error("work flow {} sub task {} exceptions",
                    this.processInstance.getId(),
                    this.taskInstance.getId(),
                    e);
        }
        return true;
    }

    @Override
    protected boolean taskTimeout() {
        TaskTimeoutStrategy taskTimeoutStrategy =
                taskDefinition.getTimeoutNotifyStrategy();
        if (TaskTimeoutStrategy.FAILED != taskTimeoutStrategy
                && TaskTimeoutStrategy.WARNFAILED != taskTimeoutStrategy) {
            return true;
        }
        logger.info("sub process task {} timeout, strategy {} ",
                taskInstance.getId(), taskTimeoutStrategy.getDescp());
        killTask();
        return true;
    }

    private void endTask() {
        if (taskState().typeIsFinished()) {
            logger.info("task instance typeIsFinished nothing[endTask] executed");
            return;
        }
        ProcessInstance subProcessInstance = processService.findSubProcessInstance(processInstance.getId(), taskInstance.getId());
        if (subProcessInstance == null) {
            logger.info("sub processInstance is null, may be creating. Wait please.");
            return;
        }

        logger.info("work flow {} task {}, sub work flow:{} state:{}",
                processInstance.getId(),
                taskInstance.getId(),
                subProcessInstance.getId(),
                subProcessInstance.getState().getDescp());
        if (!subProcessInstance.getState().typeIsFinished()) {
            return;
        }
        if (finished.compareAndSet(false, true)) {
            taskInstance.setState(subProcessInstance.getState());
            taskInstance.setEndTime(new Date());
            processService.saveTaskInstance(taskInstance);
            logger.info("sub task processor ended");
        }
    }

    private boolean runningChecked() {
        if (finished.get()) {
            return false;
        }
        long timeMs = DateUtils.differMs(new Date(), new Date(lastRunningTime));
        if (timeMs < RUNNING_INTERVAL_TIME_MILLS) {
            return false;
        }
        lastRunningTime = System.currentTimeMillis();
        return true;
    }

    @Override
    protected boolean persistTask(TaskAction taskAction) {
        switch (taskAction) {
            case STOP:
                this.killTask();
                return true;
            default:
                logger.error("unknown task action: {}", taskAction.toString());
        }
        return false;
    }

    @Override
    protected boolean pauseTask() {
        pauseSubWorkFlow();
        return true;
    }

    private boolean pauseSubWorkFlow() {
        if (taskState().typeIsFinished()) {
            return false;
        }

        if (finished.compareAndSet(false, true)) {
            ProcessInstance subProcessInstance = processService.findSubProcessInstance(processInstance.getId(), taskInstance.getId());
            if (subProcessInstance != null) {
                subProcessInstance.setState(ExecutionStatus.READY_PAUSE);
                processService.updateProcessInstance(subProcessInstance);
                sendToSubProcess(subProcessInstance);
            }
            this.taskInstance.setState(ExecutionStatus.PAUSE);
            this.taskInstance.setEndTime(new Date());
            processService.saveTaskInstance(taskInstance);
            return true;
        }
        return false;
    }

    @Override
    protected boolean killTask() {
        if (taskState().typeIsFinished()) {
            return false;
        }

        if (finished.compareAndSet(false, true)) {
            ProcessInstance subProcessInstance = processService.findSubProcessInstance(processInstance.getId(), taskInstance.getId());
            if (subProcessInstance != null ) {
                subProcessInstance.setState(ExecutionStatus.READY_STOP);
                processService.updateProcessInstance(subProcessInstance);
                sendToSubProcess(subProcessInstance);
            }

            taskInstance.setState(ExecutionStatus.KILL);
            taskInstance.setEndTime(new Date());
            processService.saveTaskInstance(taskInstance);
            
            return true;
        }
        return false;
    }

    private void sendToSubProcess(ProcessInstance subProcessInstance) {
        StateEventChangeCommand stateEventChangeCommand = new StateEventChangeCommand(
                processInstance.getId(), taskInstance.getId(), subProcessInstance.getState(), subProcessInstance.getId(), 0
        );
        String address = subProcessInstance.getHost().split(":")[0];
        int port = Integer.parseInt(subProcessInstance.getHost().split(":")[1]);
        this.stateEventCallbackService.sendResult(address, port, stateEventChangeCommand.convert2Command());
    }

    @Override
    public String getType() {
        return TaskType.SUB_PROCESS.getDesc();
    }
}

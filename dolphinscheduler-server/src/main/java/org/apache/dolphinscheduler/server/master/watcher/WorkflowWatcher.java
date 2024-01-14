package org.apache.dolphinscheduler.server.master.watcher;

import org.apache.curator.utils.ThreadUtils;
import org.apache.dolphinscheduler.common.thread.Stopper;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.dao.entity.ProcessInstance;
import org.apache.dolphinscheduler.dao.entity.TaskInstance;
import org.apache.dolphinscheduler.server.master.registry.MasterRegistryClient;
import org.apache.dolphinscheduler.server.master.runner.WorkflowExecuteThread;
import org.apache.dolphinscheduler.service.process.ProcessService;
import org.apache.hadoop.util.ThreadUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * WorkflowWatcher
 *
 * @author liliangshan
 * @date 2024-01-07
 */
public class WorkflowWatcher extends Thread {

    private static final Logger logger = LoggerFactory.getLogger(WorkflowWatcher.class);

    private final ProcessService processService;
    private final MasterRegistryClient masterRegistryClient;
    private final ConcurrentHashMap<Integer, ProcessInstanceWatcherContext> contexts = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, ProcessInstanceWatcherContext> containers = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, TaskInstance> taskInstanceRetryCheckList;
    private final ConcurrentHashMap<Integer, WorkflowExecuteThread> processInstanceExecMaps;

    private final int stateCheckIntervalSecs;
    private final int FAILURE_RETRY_AWAIT_MINUTE = 5;


    public WorkflowWatcher(ProcessService processService,
                           MasterRegistryClient masterRegistryClient,
                           ConcurrentHashMap<Integer, TaskInstance> taskRetryCheckList,
                           ConcurrentHashMap<Integer, WorkflowExecuteThread> processInstanceExecMaps,
                           int stateCheckIntervalSecs) {
        this.processService = processService;
        this.masterRegistryClient = masterRegistryClient;
        this.taskInstanceRetryCheckList = taskRetryCheckList;
        this.processInstanceExecMaps = processInstanceExecMaps;
        this.stateCheckIntervalSecs = stateCheckIntervalSecs;

    }

    @Override
    public void run() {
        while (Stopper.isRunning()) {
            logger.info("started");
            try {
                this.watchProcessInstances();
                this.watchProcessInstanceTasks();
            } catch (Exception e) {
                logger.error("run error.", e);
            }

            logger.info("end");
            ThreadUtil.sleepAtLeastIgnoreInterrupts(stateCheckIntervalSecs * 12L * 5);
        }
    }

    private void watchProcessInstanceTasks() {
        List<WorkflowExecuteThread> workflowExecuteThreads = new ArrayList<>(processInstanceExecMaps.values());
        workflowExecuteThreads.forEach(this::watchProcessInstanceTask);
    }

    private void watchProcessInstanceTask(WorkflowExecuteThread workflowExecuteThread) {
        try {
            if (workflowExecuteThread == null
                    || workflowExecuteThread.getProcessInstance() == null
                    || workflowExecuteThread.getProcessInstance().getId() <= 0
                    || workflowExecuteThread.getProcessInstance().getState().typeIsFinished()) {
                return;
            }

            int processInstanceId = workflowExecuteThread.getProcessInstance().getId();
            ProcessInstance processInstance = processService.findProcessInstanceById(processInstanceId);
            if (processInstance == null || processInstance.getState().typeIsFinished()) {
                return;
            }

            List<TaskInstance> taskInstances = processService.findValidTaskListByProcessId(processInstanceId);
            taskInstances.forEach(this::watchProcessInstancesSubProcess);
            List<TaskInstance> failedTaskInstances = taskInstances.stream()
                    .filter(it -> !it.isSubProcess())
                    .filter(it -> it.getState().typeIsFailure() && it.taskCanRetry() && it.getRetryInterval() > 0)
                    .filter(it -> {
                        long failedTimeInterval = DateUtils.differSec(new Date(), it.getEndTime());
                        long interval = (long) (it.getRetryInterval() + FAILURE_RETRY_AWAIT_MINUTE * 2) * 60;
                        return interval <= failedTimeInterval;
                    }).collect(Collectors.toList());

            failedTaskInstances.forEach(it -> {
                if (!taskInstanceRetryCheckList.containsKey(it.getId())) {
                    taskInstanceRetryCheckList.put(it.getId(), it);
                }
            });
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void watchProcessInstances() {
        this.watchProcessInstanceStateChangeEvents();
    }

    private void watchProcessInstancesSubProcess(TaskInstance taskInstance) {
        if ((taskInstance.isSubProcess() || taskInstance.isDependTask()) && taskInstance.getState().typeIsRunning()) {
            taskInstanceRetryCheckList.put(taskInstance.getId(), taskInstance);
        }
    }

    private void watchProcessInstanceStateChangeEvents() {
        try {
            List<Integer> processInstanceIds = processInstanceExecMaps.keySet().stream()
                    .filter(it -> it > 0).collect(Collectors.toList());
            processInstanceIds.forEach(it -> this.watchWorkflow(processInstanceExecMaps.get(it)));
            containers.values().forEach(this::executeContainer);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void watchWorkflow(WorkflowExecuteThread workflowExecuteThread) {
        try {
            if (workflowExecuteThread == null
                    || workflowExecuteThread.getProcessInstance() == null
                    || workflowExecuteThread.getProcessInstance().getState().typeIsFinished()) {
                return;
            }

            ProcessInstance processInstance = workflowExecuteThread.getProcessInstance();
            ProcessInstance instance = processService.findProcessInstanceById(processInstance.getId());
            if (processInstance.getState() == instance.getState()) {
                return;
            }

            if (processInstance.getState().typeIsRunning() &&
                    (instance.getState().typeIsStop() || instance.getState().typeIsReadyStop())) {
                return;
            }

            if ((processInstance.getState().typeIsSubmmittedSccess() && instance.getState().typeIsRunning())
                    || (processInstance.getState().typeIsReadyStop() && instance.getState().typeIsStop())
                    || (processInstance.getState().typeIsRunning() && instance.getState().typeIsFinished())) {
                if (!containers.containsKey(processInstance.getId())) {
                    containers.putIfAbsent(processInstance.getId(), new ProcessInstanceWatcherContext(processInstance,
                            System.currentTimeMillis()));
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void executeContainer(ProcessInstanceWatcherContext context) {
        try {
            if (context == null || context.getProcessInstance() == null) {
                return;
            }

            long interval = System.currentTimeMillis() - context.getTimeMs() - FAILURE_RETRY_AWAIT_MINUTE * 6 * 60 * 1000;
            if (interval < 0) {
                return;
            }

            WorkflowExecuteThread workflowExecuteThread = processInstanceExecMaps.get(context.getProcessInstance().getId());
            if (workflowExecuteThread == null) {
                containers.remove(context.getProcessInstance().getId());
                return;
            }

            ProcessInstance processInstance = workflowExecuteThread.getProcessInstance();
            ProcessInstance instance = processService.findProcessInstanceById(processInstance.getId());
            if (processInstance.getState() == instance.getState()) {
                containers.remove(context.getProcessInstance().getId());
                return;
            }

            if (processInstance.getState().typeIsRunning() &&
                    (instance.getState().typeIsReadyStop() || instance.getState().typeIsReadyPause())) {
                containers.remove(context.getProcessInstance().getId());
                return;
            }

            if ((processInstance.getState().typeIsSubmmittedSccess() && instance.getState().typeIsRunning())
                    || (processInstance.getState().typeIsReadyStop() && instance.getState().typeIsStop())
                    || (processInstance.getState().typeIsRunning() && instance.getState().typeIsFinished())) {
                logger.info("change instance state:{}", processInstance);
                workflowExecuteThread.updateProcessInstanceState();
            }

            containers.remove(context.getProcessInstance().getId());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


}

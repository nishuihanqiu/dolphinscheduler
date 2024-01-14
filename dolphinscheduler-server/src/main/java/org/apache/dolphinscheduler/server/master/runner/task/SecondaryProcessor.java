package org.apache.dolphinscheduler.server.master.runner.task;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.enums.*;
import org.apache.dolphinscheduler.common.utils.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.common.utils.NetUtils;
import org.apache.dolphinscheduler.dao.entity.*;
import org.apache.dolphinscheduler.remote.command.StateEventChangeCommand;
import org.apache.dolphinscheduler.remote.processor.StateEventCallbackService;
import org.apache.dolphinscheduler.server.utils.LogUtils;
import org.apache.dolphinscheduler.server.utils.SchedulerUtils;
import org.apache.dolphinscheduler.service.bean.SpringApplicationContext;
import org.h2.util.Task;

import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * SecondaryProcessor
 *
 * @author liliangshan
 * @date 2023-12-17
 */
public class SecondaryProcessor extends BaseTaskProcessor {

    private static final long RUNNING_INTERVAL_TIME_MILLS = 1000 * 30;
    private long lastRunningTime = 0L;
    private final AtomicBoolean finished = new AtomicBoolean(false);
    private final AtomicInteger subInstanceCreatedCount = new AtomicInteger(0);
    private final AtomicBoolean subInstanceCreated = new AtomicBoolean(false);
    private final ProcessDefinition processDefinition;
    private final ProcessDefinition subProcessDefinition;
    private final TaskDefinition taskDefinition;
    private final Schedule subProcessSchedule;
    private final StateEventCallbackService stateEventCallbackService = SpringApplicationContext.getBean(StateEventCallbackService.class);
    private volatile ProcessInstance subProcessInstance;
    private volatile Date currentScheduleTime;
    private volatile boolean updatedSubInstance;
    private final List<Date> scheduleDates = Lists.newArrayList();

    public SecondaryProcessor(ProcessDefinition processDefinition,
                              ProcessInstance processInstance,
                              TaskDefinition taskDefinition,
                              ProcessDefinition subProcessDefinition) {
        this.processDefinition = processDefinition;
        this.processInstance = processInstance;
        this.subProcessDefinition = subProcessDefinition;
        this.taskDefinition = taskDefinition;
        this.subProcessSchedule = this.getSubProcessSchedule(subProcessDefinition);
        this.subProcessInstance = null;
        this.currentScheduleTime = null;
    }

    @Override
    protected boolean submitTask() {
        taskInstance = processService.submitTask(taskInstance, maxRetryTimes, commitInterval);
        if (taskInstance == null) {
            return false;
        }
        this.setTaskExecutionLogger();
        this.initLogPath();

        logger.info("-------  一键重跑 -------");
        logger.info("------- 重跑工作流: {}", processDefinition.getName());
        logger.info("------- 当前任务提交成功[OK] -----------------------");
        return this.updateTask();
    }

    private boolean updateTask() {
        try {
            this.startTaskInstance();
            this.createSubProcessScheduleTime();
            this.createSubProcessInstanceCommand(processInstance.getCommandType());
            return true;
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            logger.error("执行错误");
            taskInstance.setState(ExecutionStatus.FAILURE);
            taskInstance.setEndTime(new Date());
            processService.updateTaskInstance(taskInstance);
        }
        return false;
    }

    private void initLogPath() {
        taskInstance.setLogPath(LogUtils.getTaskLogPath(processInstance.getProcessDefinitionCode(),
                processInstance.getProcessDefinitionVersion(),
                taskInstance.getProcessInstanceId(),
                taskInstance.getId()));
    }

    private void startTaskInstance() {
        taskInstance.setHost(NetUtils.getAddr(masterConfig.getListenPort()));
        taskInstance.setState(ExecutionStatus.RUNNING_EXECUTION);
        taskInstance.setSubmitTime(new Date());
        taskInstance.setStartTime(new Date());
        processService.updateTaskInstance(taskInstance);
        logger.info("---------> 当前任务开始执行[RUNNING]  ----------------");
        logger.info("---------------------------------------------------");
    }

    private Schedule getSubProcessSchedule(ProcessDefinition subProcessDefinition) {
        long subProcessDefinitionCode = subProcessDefinition.getCode();
        Schedule subProcessSchedule = processService.queryByProcessDefinitionCode(subProcessDefinitionCode);
        if (subProcessSchedule != null && subProcessSchedule.getReleaseState() == ReleaseState.ONLINE) {
            return subProcessSchedule;
        }

        List<Schedule> schedules = processService.queryOnlineScheduleBySubProcessDefinition(subProcessDefinition);
        if (schedules == null || schedules.isEmpty()) {
            return null;
        }
        return schedules.stream().max((o1, o2) -> {
            if (o1.getId() > o2.getId()) {
                return 1;
            }
            return o1.getId() == o2.getId() ? 0 : -1;
        }).orElse(schedules.get(0));
    }

    private void createSubProcessScheduleTime() {
        if (taskInstance.getState().typeIsFinished()) {
            return;
        }
        if (subProcessSchedule == null || subProcessSchedule.getReleaseState() == ReleaseState.OFFLINE) {
            return;
        }

        Date scheduleDate = this.getStartScheduleTime();
        scheduleDates.addAll(SchedulerUtils.getScheduleFireDateFromStart(subProcessSchedule.getCrontab(), scheduleDate));

        ProcessInstanceMap instanceMap = processService.findWorkProcessMapByParent(processInstance.getId(), taskInstance.getId());
        if (null != instanceMap && (
                CommandType.RECOVER_TOLERANCE_FAULT_PROCESS == processInstance.getCommandType()
                || CommandType.START_FAILURE_TASK_PROCESS == processInstance.getCommandType()
                || CommandType.RECOVER_SUSPENDED_PROCESS == processInstance.getCommandType())) {
            ProcessInstance childInstance = processService.findProcessInstanceById(instanceMap.getProcessInstanceId());
            if (childInstance != null) {
                this.currentScheduleTime = childInstance.getScheduleTime();
                if (this.currentScheduleTime != null) {
                    this.subInstanceCreatedCount.set(scheduleDates.indexOf(currentScheduleTime));
                    this.subInstanceCreated.set(false);
                    logger.info("-------> 任务工作流{}, 执行调度时间: {}",
                            processInstance.getCommandType().getDescp(), DateUtils.dateToString(currentScheduleTime));
                    return;
                }
            }
        }

        this.currentScheduleTime = scheduleDates.isEmpty() ? null : scheduleDates.get(0);
        this.subInstanceCreatedCount.set(0);
        logger.info("----------> 任务重跑工作流[补数重跑], 执行调度时间: {}",
                currentScheduleTime == null ? "NULL" : DateUtils.dateToString(currentScheduleTime));
    }

    private Date getStartScheduleTime() {
        Date defaultDate = processInstance.getScheduleTime() == null ?
                DateUtils.getStartOfDay(processInstance.getStartTime()) : processInstance.getScheduleTime();
        return DateUtils.getStartOfDay(defaultDate);
    }

    private void createSubProcessInstanceCommand(CommandType commandType) {
        if (subInstanceCreated.get()) {
            return;
        }

        if (this.currentScheduleTime == null) {
            if (subProcessSchedule != null && subProcessSchedule.getReleaseState() == ReleaseState.OFFLINE) {
                logger.info("任务重跑工作流 code:{}, name:{}, crontab:{}, 定时器处于： OFFLINE，执行调度时间 IS NULL，子工作流实例命令不会被创建，将不会有子工作流执行。",
                        subProcessDefinition.getCode(), subProcessDefinition.getName(), subProcessSchedule.getCrontab());
                return;
            }

            logger.info("任务重跑工作流：code:{}, name:{}, crontab:{}, 定时器处于： OFFLINE，执行调度时间 IS NULL，子工作流实例命令不会被创建",
                    subProcessDefinition.getCode(), subProcessDefinition.getName(),
                    subProcessSchedule == null ? "NULL" : subProcessSchedule.getCrontab());
            return;
        }

        if (subInstanceCreated.compareAndSet(false, true)) {
            int count = subInstanceCreatedCount.incrementAndGet();
            if (count > scheduleDates.size()) {
                logger.info("sub process instance count[created] {} gt schedule dates size:{}", count, scheduleDates.size());
                return;
            }

            if (!SchedulerUtils.nowAfter(this.currentScheduleTime)) {
                logger.info("now:{} is before schedule time:{} sub process instance command not be created",
                        DateUtils.dateToString(new Date()),
                        DateUtils.dateToString(this.currentScheduleTime));
                return;
            }

            this.handleRunningSubProcessInstance(this.currentScheduleTime);
            ProcessInstance parentInstance = this.copyParentProcessInstanceWithScheduleTime(this.currentScheduleTime, commandType);

            try {
                logger.info("创建重跑子工作流[code: {}, name:{}, crontab:{}, 调度时间:{}] 指令command",
                        subProcessDefinition.getCode(),
                        subProcessDefinition.getName(),
                        subProcessSchedule.getCrontab(),
                        DateUtils.dateToString(currentScheduleTime));
                processService.createSecondarySubWorkProcess(parentInstance, taskInstance);
                logger.info("sub process code: {}, name:{}, crontab:{}, 调度时间:{} command created ",
                        subProcessDefinition.getCode(),
                        subProcessDefinition.getName(),
                        subProcessSchedule.getCrontab(),
                        DateUtils.dateToString(currentScheduleTime));
            } catch (Exception e) {
                logger.info("创建重跑子工作流[code: {}, name:{}, crontab:{}, 调度时间:{}] 指令command error: {}",
                        subProcessDefinition.getCode(),
                        subProcessDefinition.getName(),
                        subProcessSchedule.getCrontab(),
                        DateUtils.dateToString(currentScheduleTime),
                        e.getMessage());
                subInstanceCreated.set(false);
            }
        }
    }

    private void handleRunningSubProcessInstance(Date scheduleTime) {
        if (scheduleTime == null) {
            return;
        }

        long processDefinitionCode = subProcessDefinition.getCode();
        long projectCode = subProcessDefinition.getProjectCode();
        logger.info("query latest 10 instances, projectCode:{}, processDefinitionCode:{}, scheduleTime:{}",
                projectCode, processDefinitionCode, DateUtils.dateToString(scheduleTime));
        List<ProcessInstance> instances = processService.queryTenByProcessDefinitionCodeAndSchedulerTime(projectCode,
                processDefinitionCode, scheduleTime);
        logger.info("query latest 10 instances count:{}", instances.size());

        if (instances.isEmpty()) {
            logger.info("当前重跑工作流 projectCode:{}, processDefinitionCode:{}, scheduleTime:{}, 未有正在运行的工作流实例....",
                    projectCode, processDefinitionCode, DateUtils.dateToString(scheduleTime));
            return;
        }

        List<Integer> subProcessInstances = instances.stream().filter(ProcessInstance::isSubProcessInstance)
                .map(ProcessInstance::getId)
                .collect(Collectors.toList());
        List<ProcessInstance> parentProcessInstances = instances.stream()
                .filter(it ->
                    (it.getScheduleTime() != null && it.getScheduleTime().getTime() == scheduleTime.getTime())
                            || it.getStartTime().after(DateUtils.getStartOfDay(new Date())))
                .filter(it -> !it.isSubProcessInstance())
                .collect(Collectors.toList());
        List<ProcessInstance> otherParentInstances = processService.queryInstancesBySubProcessIds(subProcessInstances);
        parentProcessInstances.addAll(otherParentInstances.stream()
                .filter(it -> it.getState().typeIsRunning() || it.getState() == ExecutionStatus.SUBMITTED_SUCCESS)
                .filter(it -> it.getId() != processInstance.getId())
                .collect(Collectors.toList()));

        if (parentProcessInstances.isEmpty()) {
            logger.info("当前重跑工作流 projectCode:{}, processDefinitionCode:{}, scheduleTime:{} parent instance count:0, 未有正在运行的工作流实例....",
                    projectCode, processDefinitionCode, DateUtils.dateToString(scheduleTime));
        }
        parentProcessInstances.forEach(this::stopProcessInstance);
    }

    private void stopProcessInstance(ProcessInstance instance) {
        if (instance == null || instance.getState().typeIsFinished()
                || instance.getState() == ExecutionStatus.READY_STOP
                || instance.getState() == ExecutionStatus.READY_PAUSE) {
            logger.info("instance:{}, scheduleTime:{}, state:{}, nothing should be stopped...",
                    instance == null ? "none" : instance.getName(),
                    instance == null ? "none" : instance.getState().getDescp(),
                    (instance == null || instance.getScheduleTime() == null) ? "none" :
                    DateUtils.dateToString(instance.getScheduleTime()));
            return;
        }

        logger.info("instance:{} current state:{}, scheduleTime:{}, isSubProcess:{}",
                instance.getName(),
                instance.getState().getDescp(),
                instance.getScheduleTime() == null ? "none" : DateUtils.dateToString(instance.getScheduleTime()),
                instance.getIsSubProcess().getDescp());

        if (instance.getIsSubProcess() == Flag.YES) {
            return;
        }

        logger.info("current task name:{}, id:{} \n kill exists running sub process instance ---->\n" +
                "------------> code:{} name:{} id:{} instance name:{} state:{}",
                taskInstance.getName(),
                taskInstance.getId(),
                subProcessDefinition.getCode(),
                subProcessDefinition.getName(),
                instance.getId(),
                instance.getName(),
                instance.getState().getDescp());
        logger.info("------------------------------------------------------------------------------");

        instance.setCommandType(CommandType.STOP);
        instance.addHistoryCmd(CommandType.STOP);
        instance.setState(ExecutionStatus.READY_STOP);
        instance.setEndTime(new Date());

        int updatedCount = processService.updateProcessInstance(instance);
        if (updatedCount == 0) {
            return;
        }

        StateEventChangeCommand command = new StateEventChangeCommand(
                instance.getId(), 0, instance.getState(), instance.getId(), 0);
        logger.info("send state event change command {}", JSONUtils.toJsonString(command));
        logger.info("------------------------------------------------------------------------------");
        this.sendResult(command, instance.getHost());
        logger.info("stop sub process instance send command end.");
    }

    private ProcessInstance copyParentProcessInstanceWithScheduleTime(Date scheduleTime, CommandType commandType) {
        ProcessInstance instance = JSONUtils.parseObject(JSONUtils.toJsonString(processInstance), ProcessInstance.class);
        if (instance == null) {
            return null;
        }

        instance.setScheduleTime(scheduleTime);
        instance.setCommandType(commandType);
        return instance;
    }

    private void updateTaskInstanceToRunning() {
        taskInstance.setHost(NetUtils.getAddr(masterConfig.getListenPort()));
        taskInstance.setState(ExecutionStatus.RUNNING_EXECUTION);
        taskInstance.setSubmitTime(null);
        taskInstance.setStartTime(new Date());
        processService.updateTaskInstance(taskInstance);
    }

    @Override
    protected boolean persistTask(TaskAction taskAction) {
        switch (taskAction) {
            case STOP:
                this.killTask();
                return true;
            default:
                logger.error("unkown task action:{}", taskAction);
        }
        return false;
    }

    @Override
    protected boolean pauseTask() {
        if (taskState().typeIsFinished()) {
            return false;
        }
        if (finished.compareAndSet(false, true)) {
            this.updateStateAndNoticeSubProcess(ExecutionStatus.READY_PAUSE);
            taskInstance.setState(ExecutionStatus.PAUSE);
            taskInstance.setEndTime(new Date());
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
            this.updateStateAndNoticeSubProcess(ExecutionStatus.READY_STOP);
            taskInstance.setState(ExecutionStatus.KILL);
            taskInstance.setEndTime(new Date());
            processService.saveTaskInstance(taskInstance);
            return true;
        }
        return false;
    }

    private void updateStateAndNoticeSubProcess(ExecutionStatus status) {
        ProcessInstance instance = this.getSubProcessInstance();
        if (instance != null) {
            instance.setState(status);
            processService.updateProcessInstance(instance);
            this.sendToSubProcess(instance);
        }
    }

    private void sendToSubProcess(ProcessInstance subProcessInstance) {
        StateEventChangeCommand command = new StateEventChangeCommand(
                processInstance.getId(),
                taskInstance.getId(),
                subProcessInstance.getState(),
                subProcessInstance.getId(),
                0);
        this.sendResult(command, subProcessInstance.getHost());
    }

    private void sendResult(StateEventChangeCommand command, String host) {
        if (command == null || StringUtils.isBlank(host)) {
            return;
        }
        String address = host.split(":")[0];
        int port = Integer.parseInt(host.split(":")[1]);
        this.stateEventCallbackService.sendResult(address, port, command.convert2Command());
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

    private void showSubProcessProgress(ProcessInstance subProcessInstance) {
        if (taskState().typeIsFinished()) {
            logger.info("task instance state is finished nothing is executed");
            return;
        }

        if (subProcessInstance == null) {
            logger.info("secondary sub process[code:{}] instance is null, may be creating. Wait please.",
                    subProcessDefinition.getCode());
            return;
        }

        logger.info("current task name:{} id:{} \n secondary sub process instance ---->\n" +
                "---------------------->  code:{} \n" +
                "---------------------->  process definition name:{} \n" +
                "---------------------->  id:{} \n" +
                "---------------------->  instance name:{} \n" +
                "---------------------->  state:{} \n",
                taskInstance.getName(),
                taskInstance.getId(),
                subProcessDefinition.getCode(),
                subProcessDefinition.getName(),
                subProcessInstance.getId(),
                subProcessInstance.getName(),
                subProcessInstance.getState().getDescp());
        logger.info("--------------------------------------------------------------------------------------");
    }

    private ProcessInstance getSubProcessInstance() {
        return processService.findSubProcessInstance(processInstance.getId(), taskInstance.getId());
    }

    @Override
    protected boolean runTask() {
        try {
            if (this.checkSubProcessRunning()) {
                return true;
            }
            this.endTask(this.subProcessInstance);
            return true;
        } catch (Exception e) {
            logger.error("work flow {} re sub process task {} exception", processInstance.getId(),
                    taskInstance.getId(), e);
        }
        return true;
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


    private void endTask(ProcessInstance subInstance) {
        if (finished.get()) {
            return;
        }

        if (subInstance == null) {
            this.skipAndTerminalTask();
            return;
        }

        if (!subInstance.getState().typeIsFinished()) {
            return;
        }

        if (this.gotoNextScheduleTime(subInstance)) {
            this.createSubProcessInstanceCommand(CommandType.START_PROCESS);
            return;
        }

        if (finished.compareAndSet(false, true)) {
            logger.info("secondary process instance ------->\n" +
                            "----------------> code:{}\n" +
                            "----------------> name:{}\n" +
                            "----------------> id:{}\n" +
                            "----------------> state:{}\n",
                    subInstance.getProcessDefinitionCode(),
                    subInstance.getName(),
                    subInstance.getId(),
                    subInstance.getState().getDescp());

            taskInstance.setState(subInstance.getState());
            taskInstance.setEndTime(new Date());
            processService.saveTaskInstance(taskInstance);
            logger.info("当前sub task 任务对应的子工作流实例执行完成，执行调度时间：{}，任务状态:{},当前任务执行结束",
                    DateUtils.dateToString(subInstance.getScheduleTime()),
                    subInstance.getState().getDescp());

            logger.info("--------------> 子工作流任务执行结束。。。。。");
            logger.info("--------------> secondary sub process task end。。。。。");
            logger.info("--------------------------------------------------------");
        }
    }

    private void skipAndTerminalTask() {
        if (finished.get()) {
            return;
        }
        if (SchedulerUtils.nowAfter(this.currentScheduleTime)) {
            return;
        }

        if (finished.compareAndSet(false, true)) {
            logger.info("当前任务不会创建 sub process instance, 任务状态置为成功，直接PASS，继续后续任务执行。。。");
            logger.info("sub process definition code:{}, name:{}, schedule time not at, crontab:{}",
                    subProcessDefinition.getCode(),
                    subProcessDefinition.getName(),
                    subProcessSchedule == null ? "IS NULL" : subProcessSchedule.getCrontab());
            taskInstance.setState(ExecutionStatus.SUCCESS);
            taskInstance.setEndTime(new Date());
            processService.saveTaskInstance(taskInstance);
            logger.info("secondary sub task PASS[STATE: SUCCESS],should be terminal.");
        }
    }

    private boolean gotoNextScheduleTime(ProcessInstance subInstance) {
        if (finished.get() || this.currentScheduleTime == null || subInstance == null
                || !subInstance.getState().typeIsFinished()) {
            return false;
        }
        if (!subInstance.getState().typeIsSuccess()) {
            return false;
        }
        if (this.subInstanceCreatedCount.get() >= scheduleDates.size()) {
            return false;
        }
        Date nextScheduleTime = scheduleDates.get(subInstanceCreatedCount.get());
        if (!SchedulerUtils.nowAfter(nextScheduleTime)) {
            return false;
        }

        if (subInstanceCreated.compareAndSet(true, false)) {
            this.currentScheduleTime = nextScheduleTime;
            this.subInstanceCreated.set(false);
            this.subProcessInstance = null;
            this.updatedSubInstance = false;
            logger.info("finish sub process instance [state:{}] current schedule time:{} go to next schedule time:{}",
                    subInstance.getState().getDescp(),
                    DateUtils.dateToString(subInstance.getScheduleTime()),
                    DateUtils.dateToString(nextScheduleTime));
        }
        return true;
    }

    private boolean checkSubProcessRunning() {
        if (finished.get()) {
            return false;
        }
        if (this.currentScheduleTime == null) {
            return false;
        }
        if (!SchedulerUtils.nowAfter(this.currentScheduleTime)) {
            return false;
        }

        long timeMs = DateUtils.differMs(System.currentTimeMillis(), lastRunningTime);
        this.subProcessInstance = this.getSubProcessInstance();
        if (this.subProcessInstance == null) {
            this.showSubProcessProgress(subProcessInstance);
            return true;
        }
        this.updateLog(subProcessInstance);
        if (timeMs >= RUNNING_INTERVAL_TIME_MILLS) {
            lastRunningTime = System.currentTimeMillis();
            this.showSubProcessProgress(subProcessInstance);
            return subProcessInstance.getState().typeIsRunning();
        }

        if (subProcessInstance != null && subProcessInstance.getState().typeIsFinished()) {
            this.showSubProcessProgress(subProcessInstance);
            return false;
        }

        return true;
    }

    private void updateLog(ProcessInstance subProcessInstance) {
        if (subProcessInstance != null && !updatedSubInstance) {
            ProcessInstanceMapLog processInstanceMapLog = new ProcessInstanceMapLog();
            processInstanceMapLog.setParentProcessInstanceId(processInstance.getId());
            processInstanceMapLog.setParentTaskInstanceId(taskInstance.getId());
            processInstanceMapLog.setProcessInstanceId(subProcessInstance.getId());
            processInstanceMapLog.setScheduleTime(currentScheduleTime);
            processService.updateWorkProcessInstanceMapLog(processInstanceMapLog);
            updatedSubInstance = true;
        }
    }


    @Override
    public ExecutionStatus taskState() {
        return taskInstance.getState();
    }

    @Override
    public String getType() {
        return TaskType.SUB_PROCESS.getDesc();
    }
}

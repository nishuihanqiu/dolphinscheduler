package org.apache.dolphinscheduler.dao.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

/**
 * ProcessInstanceMapLog
 *
 * @author liliangshan
 * @date 2024-01-07
 */
@TableName("t_ds_relation_process_instance_log")
public class ProcessInstanceMapLog extends ProcessInstanceMap {

    public ProcessInstanceMapLog() {

    }

    public ProcessInstanceMapLog(ProcessInstanceMap processInstanceMap) {
        this.setParentProcessInstanceId(processInstanceMap.getParentProcessInstanceId());
        this.setProcessInstanceId(processInstanceMap.getProcessInstanceId());
        this.setParentTaskInstanceId(processInstanceMap.getParentTaskInstanceId());
    }

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date scheduleTime;

    public void setScheduleTime(Date scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

    public Date getScheduleTime() {
        return scheduleTime;
    }

    @Override
    public String toString() {
        return "ProcessInstanceMap{" +
                "id=" + getId() +
                ", parentProcessInstanceId=" + getParentProcessInstanceId()  +
                ", parentTaskInstanceId=" + getParentTaskInstanceId() +
                ", processInstanceId=" + getProcessInstanceId() +
                "scheduleTime=" + getScheduleTime() +
                '}';
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        ProcessInstanceMapLog that = (ProcessInstanceMapLog) o;

        if (this.getId() != that.getId()) {
            return false;
        }
        if (getParentProcessInstanceId() != that.getParentProcessInstanceId()) {
            return false;
        }
        if (getParentTaskInstanceId() != that.getParentTaskInstanceId()) {
            return false;
        }

        if (getProcessInstanceId() != that.getProcessInstanceId()) {
            return false;
        }

        if (getScheduleTime() != null && !this.getScheduleTime().equals(that.getScheduleTime())) {
            return false;
        }
        return getScheduleTime() == that.getScheduleTime();
    }

    @Override
    public int hashCode() {
        int result = getId();
        result = 31 * result + getParentProcessInstanceId();
        result = 31 * result + getParentTaskInstanceId();
        result = 31 * result + getProcessInstanceId();
        result = 31 * result + (getScheduleTime() == null ? 0 : getScheduleTime().hashCode());
        return result;
    }

}

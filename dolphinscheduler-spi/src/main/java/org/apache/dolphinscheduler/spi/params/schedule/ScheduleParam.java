package org.apache.dolphinscheduler.spi.params.schedule;

import com.fasterxml.jackson.annotation.JsonFormat;

import java.util.Date;

/**
 * ScheduleParam
 *
 * @author liliangshan
 * @date 2023-12-17
 */
public class ScheduleParam {

    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date startTime;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss", timezone = "GMT+8")
    private Date endTime;

    private String crontab;

    private String timezoneId;

    public ScheduleParam() {
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public String getCrontab() {
        return crontab;
    }

    public void setCrontab(String crontab) {
        this.crontab = crontab;
    }

    public String getTimezoneId() {
        return timezoneId;
    }

    public void setTimezoneId(String timezoneId) {
        this.timezoneId = timezoneId;
    }

    @Override
    public String toString() {
        return "ScheduleParam{" +
                "startTime=" + startTime +
                ", endTime=" + endTime +
                ", crontab='" + crontab + '\'' +
                ", timezoneId='" + timezoneId + '\'' +
                '}';
    }
}

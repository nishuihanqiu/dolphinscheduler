package org.apache.dolphinscheduler.server.utils;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.common.Constants;
import org.apache.dolphinscheduler.common.utils.DateUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.service.quartz.cron.CronUtils;
import org.apache.dolphinscheduler.spi.params.schedule.ScheduleParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * SchedulerUtils
 *
 * @author liliangshan
 * @date 2023-12-17
 */
public class SchedulerUtils {

    private static final Logger logger = LoggerFactory.getLogger(SchedulerUtils.class);

    public static List<Date> createScheduleDate(String schedule) {
        ScheduleParam scheduleParam = JSONUtils.parseObject(schedule, ScheduleParam.class);
        if (scheduleParam == null) {
            return Collections.emptyList();
        }

        Date startTime = scheduleParam.getStartTime();
        Date endTime = scheduleParam.getEndTime();
        return CronUtils.getSelfFireDateList(startTime, endTime, scheduleParam.getCrontab());
    }

    public static List<Date> getScheduleFireDate(String crontab, Date day) {
        if (StringUtils.isBlank(crontab)) {
            return Collections.emptyList();
        }

        Date startTime = new Date(DateUtils.getStartOfDay(day).getTime() - Constants.SECOND_TIME_MILLIS);
        Date endTime = new Date(DateUtils.getEndOfDay(day).getTime() + Constants.SECOND_TIME_MILLIS);
        return getScheduleFireDate(crontab, startTime, endTime);
    }

    public static List<Date> getScheduleFireDateFromStart(String crontab, Date start) {
        if (StringUtils.isBlank(crontab)) {
            return Collections.emptyList();
        }

        Date startTime = new Date(start.getTime() - Constants.SECOND_TIME_MILLIS);
        Date endTime = new Date(DateUtils.getEndOfDay(start).getTime() + Constants.SECOND_TIME_MILLIS);
        return getScheduleFireDate(crontab, startTime, endTime);
    }

    public static List<Date> getScheduleFireDate(String crontab, Date startTime, Date endTime) {
        if (StringUtils.isBlank(crontab)) {
            return Collections.emptyList();
        }
        Map<String, String> map = Maps.newHashMap();
        map.put("startTime", DateUtils.dateToString(startTime));
        map.put("endTime", DateUtils.dateToString(endTime));
        map.put("crontab", crontab);

        String text = JSONUtils.toJsonString(map);
        logger.info(JSONUtils.toJsonString(map));
        return createScheduleDate(text);
    }

    public static boolean nowAfter(Date when) {
        if (when == null) {
            return false;
        }
        Date now = new Date();
        return now.after(when);
    }
}

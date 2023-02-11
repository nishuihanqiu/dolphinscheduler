package org.apache.dolphinscheduler.common.task.kylin;

import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.common.process.ResourceInfo;
import org.apache.dolphinscheduler.common.task.AbstractParameters;

import java.util.List;

/**
 * KylinParameters
 *
 * @author liliangshan
 * @date 2023-02-11
 */
public class KylinParameters extends AbstractParameters {

    private String kylinHost;
    private String project;
    private String cube;
    private String startTime;
    private String endTime;
    private String command;
    private Boolean autoConvertToRefresh = false;

    @Override
    public boolean checkParameters() {
        return StringUtils.isNotBlank(kylinHost)
                && StringUtils.isNotBlank(project)
                && StringUtils.isNotBlank(cube)
                && StringUtils.isNotBlank(startTime)
                && StringUtils.isNotBlank(endTime)
                && StringUtils.isNotBlank(command);
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return Lists.newArrayList();
    }

    public String getKylinHost() {
        return kylinHost;
    }

    public void setKylinHost(String kylinHost) {
        this.kylinHost = kylinHost;
    }

    public String getProject() {
        return project;
    }

    public void setProject(String project) {
        this.project = project;
    }

    public String getCube() {
        return cube;
    }

    public void setCube(String cube) {
        this.cube = cube;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getCommand() {
        return command;
    }

    public void setCommand(String command) {
        this.command = command;
    }

    public Boolean getAutoConvertToRefresh() {
        return autoConvertToRefresh;
    }

    public void setAutoConvertToRefresh(Boolean autoConvertToRefresh) {
        this.autoConvertToRefresh = autoConvertToRefresh;
    }
}

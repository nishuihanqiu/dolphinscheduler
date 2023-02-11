package org.apache.dolphinscheduler.common.task.waterdrop;

import com.google.common.collect.Sets;
import org.apache.commons.lang.StringUtils;
import org.apache.dolphinscheduler.common.process.ResourceInfo;
import org.apache.dolphinscheduler.common.task.AbstractParameters;

import java.util.List;
import java.util.Set;

/**
 * SeaTunnelParameters
 *
 * @author liliangshan
 * @date 2023-02-11
 */
public class SeaTunnelParameters extends AbstractParameters {

    private static final String DEPLOY_CLIENT = "client";
    private static final String DEPLOY_CLUSTER = "cluster";
    private static final String DEPLOY_LOCAL = "local";
    private static final Set<String> deployModes = Sets.newHashSet(DEPLOY_CLIENT, DEPLOY_CLUSTER, DEPLOY_LOCAL);
    private static final Set<String> engines = Sets.newHashSet("yarn", "local", "spark://", "mesos://");
    private static final String DEFAULT_QUEUE = "default";

    private String customConf;
    private String deployMode;
    private String master;
    private String masterUrl;
    private String queue;
    private String rawScript;
    private List<ResourceInfo> resourceList;

    @Override
    public boolean checkParameters() {
        if (StringUtils.isBlank(queue)) {
            queue = DEFAULT_QUEUE;
        }
        return StringUtils.isNotBlank(customConf)
                && StringUtils.isNotBlank(deployMode)
                && StringUtils.isNotBlank(master)
                && deployModes.contains(StringUtils.trimToEmpty(deployMode))
                && engines.contains(StringUtils.trimToEmpty(master));
    }

    @Override
    public List<ResourceInfo> getResourceFilesList() {
        return resourceList;
    }

    public String getCustomConf() {
        return customConf;
    }

    public void setCustomConf(String customConf) {
        this.customConf = customConf;
    }

    public String getDeployMode() {
        return deployMode;
    }

    public void setDeployMode(String deployMode) {
        this.deployMode = deployMode;
    }

    public String getMaster() {
        return master;
    }

    public void setMaster(String master) {
        this.master = master;
    }

    public String getMasterUrl() {
        return masterUrl;
    }

    public void setMasterUrl(String masterUrl) {
        this.masterUrl = masterUrl;
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getRawScript() {
        return rawScript;
    }

    public void setRawScript(String rawScript) {
        this.rawScript = rawScript;
    }

    public void setResourceList(List<ResourceInfo> resourceList) {
        this.resourceList = resourceList;
    }

    public List<ResourceInfo> getResourceList() {
        return resourceList;
    }

}

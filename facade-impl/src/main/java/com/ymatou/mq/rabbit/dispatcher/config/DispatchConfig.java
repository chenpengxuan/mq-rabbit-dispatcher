/**
 * (C) Copyright 2016 Ymatou (http://www.ymatou.com/).
 *
 * All rights reserved.
 */
package com.ymatou.mq.rabbit.dispatcher.config;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Component;

import com.baidu.disconf.client.common.annotations.DisconfFile;
import com.baidu.disconf.client.common.annotations.DisconfFileItem;

@Component
@DisconfFile(fileName = "dispatch.properties")
public class DispatchConfig {

    private String groupId;

    private String basicQos;

    /**
     * @return the groupId
     */
    @DisconfFileItem(name = "groupid")
    public String getGroupId() {
        return groupId;
    }

    /**
     * @param groupId the groupId to set
     */
    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    @DisconfFileItem(name = "basic.qos")
    public String getBasicQos() {
        return basicQos;
    }

    public void setBasicQos(String basicQos) {
        this.basicQos = basicQos;
    }

    public boolean isMatch(String dispatchGroup) {
        if (StringUtils.isNotBlank(dispatchGroup) && dispatchGroup.contains(groupId)) {
            return true;
        }
        return false;
    }
}

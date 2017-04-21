/*
 *
 *  (C) Copyright 2017 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.config;

import com.baidu.disconf.client.DisconfMgrBean;
import com.baidu.disconf.client.DisconfMgrBeanSecond;
import com.ymatou.mq.rabbit.dispatcher.util.Constants;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

/**
 * @author luoshiqian 2016/8/30 15:49
 */
@Configuration
public class DisconfMgr {

    @Bean(name = "disconfMgrBean", destroyMethod = "destroy")
    public DisconfMgrBean disconfMgrBean(TomcatConfig tomcatConfig) {

        DisconfMgrBean disconfMgrBean = new DisconfMgrBean();
        disconfMgrBean.setScanPackage("com.ymatou.mq");

        disconfMgrBean.setDisconfInitCallback(() -> {
            Constants.TOMCAT_CONFIG = tomcatConfig;
        });
        return disconfMgrBean;
    }
}

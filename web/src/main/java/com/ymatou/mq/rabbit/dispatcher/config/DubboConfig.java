/*
 *
 *  (C) Copyright 2017 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;
import org.springframework.context.annotation.ImportResource;

/**
 * @author luoshiqian 2016/8/30 19:17
 */
@Configuration
@ImportResource("classpath:spring/dubbo-provider.xml")
@DependsOn("disconfMgrBean2")
public class DubboConfig {
}

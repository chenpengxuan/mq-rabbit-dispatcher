/*
 *
 * (C) Copyright 2017 Ymatou (http://www.ymatou.com/). All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.starter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import com.ymatou.mq.rabbit.dispatcher.util.Constants;



/**
 * @author luoshiqian 2016/8/31 12:31
 */
@Configuration
@EnableAspectJAutoProxy(proxyTargetClass = true)
@ComponentScan(basePackages = "com.ymatou")
public class Application {
    public static final Logger logger = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        Constants.ctx = new AnnotationConfigApplicationContext(Application.class);
        try {
            Constants.ctx.start();
        } catch (Exception e) {
            logger.warn("shut down ", e);
        }
    }
}

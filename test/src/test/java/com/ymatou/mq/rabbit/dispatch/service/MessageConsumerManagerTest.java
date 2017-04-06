/*
 *
 *  (C) Copyright 2017 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatch.service;

import com.ymatou.mq.rabbit.dispatcher.service.MessageConsumerManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import static org.junit.Assert.fail;

/**
 * Created by zhangzhihua on 2017/3/29.
 */
/**
 * @author luoshiqian 2017/3/27 16:34
 */
@Configuration
@EnableAspectJAutoProxy
@ComponentScan(basePackages = "com.ymatou")
public class MessageConsumerManagerTest {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerManagerTest.class);

    AnnotationConfigApplicationContext ctx;

    MessageConsumerManager messageConsumerManager;

    void init(){
        ctx = new AnnotationConfigApplicationContext(MessageConsumerManagerTest.class);
        messageConsumerManager = (MessageConsumerManager)ctx.getBean("messageConsumerManager");
    }

    public static void main(String[] args){
        MessageConsumerManagerTest test = new MessageConsumerManagerTest();
        test.init();
    }

}

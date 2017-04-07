/*
 *
 *  (C) Copyright 2017 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatch.service;

import com.alibaba.fastjson.JSONObject;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.infrastructure.util.NetUtil;
import com.ymatou.mq.rabbit.dispatch.BaseTest;
import com.ymatou.mq.rabbit.dispatcher.service.MessageConsumerManager;
import com.ymatou.mq.rabbit.dispatcher.service.MessageDispatchService;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

import java.util.Date;

import static org.junit.Assert.fail;

/**
 * Created by zhangzhihua on 2017/3/29.
 */
@Configuration
@EnableAspectJAutoProxy
@ComponentScan(basePackages = "com.ymatou")
public class MessageDispatchServiceTest{

    AnnotationConfigApplicationContext ctx;

    MessageDispatchService messageDispatchService;

    void init(){
        ctx = new AnnotationConfigApplicationContext(MessageDispatchServiceTest.class);
        messageDispatchService = (MessageDispatchService)ctx.getBean("messageDispatchService");
    }

    public void testDispatch(){
        Message message = this.buildMessage();
        try {
            messageDispatchService.dispatch(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    Message buildMessage(){
        Message msg = new Message();
        msg.setAppId("rabbit_optimization");
        msg.setQueueCode("biz1");
        msg.setId(ObjectId.get().toString());
        msg.setBizId(ObjectId.get().toString());
        JSONObject jsonObject = new JSONObject();
        jsonObject.put("name","kaka-" + new Date());
        msg.setBody(jsonObject.toJSONString());
        msg.setClientIp("172.16.22.102");
        msg.setRecvIp(NetUtil.getHostIp());
        msg.setCreateTime(new Date());
        return msg;
    }

    public static void main(String[] args){
        MessageDispatchServiceTest test = new MessageDispatchServiceTest();
        test.init();
        test.testDispatch();
        try {
            Thread.sleep(1000*1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}

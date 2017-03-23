/*
 *
 * (C) Copyright 2017 Ymatou (http://www.ymatou.com/). All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.facade.impl;



import com.alibaba.dubbo.config.annotation.Service;
import com.ymatou.mq.rabbit.dispatcher.facade.DemoFacade;
import org.springframework.stereotype.Component;


/**
 * @author luoshiqian 2016/8/31 14:13
 */
@Service(protocol = "dubbo")
@Component
public class DemoFacadeImpl implements DemoFacade {

    @Override
    public String sayHello(String name) {
        return "hello world:" + name;
    }
}

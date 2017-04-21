/*
 *
 *  (C) Copyright 2017 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatch;

import com.ymatou.mq.rabbit.dispatcher.util.Constants;
import org.junit.Test;
import org.springframework.context.annotation.*;

/**
 * @author luoshiqian 2017/4/20 18:59
 */
@Configuration
@EnableAspectJAutoProxy(proxyTargetClass = true)
@ComponentScan(basePackages = "com.ymatou.mq")
@ImportResource("classpath:spring/dubbo-provider.xml")
public class BaseTestWithDubbo {

    @Test
    public void startup()throws Exception{
        Constants.ctx = new AnnotationConfigApplicationContext(BaseTestWithDubbo.class);

        Constants.ctx.start();

        System.in.read();
    }
}

/*
 *
 *  (C) Copyright 2016 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.facade.rest;

/**
 * @author luoshiqian 2016/8/31 14:12
 */
public interface DemoResource {

    String sayHello(String name);

    String testShutdownGracefully();

    String shutdown();


}

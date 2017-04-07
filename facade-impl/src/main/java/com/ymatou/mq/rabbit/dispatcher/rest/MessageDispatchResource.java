/*
 *
 *  (C) Copyright 2016 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.rest;

/**
 * @author luoshiqian 2016/8/31 14:12
 */
public interface MessageDispatchResource {

    String sayHello(String name);

    String testShutdownGracefully();

    String shutdown();


}

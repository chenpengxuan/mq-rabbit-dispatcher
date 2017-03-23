/*
 *
 *  (C) Copyright 2016 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.facade.rest;

import java.util.concurrent.TimeUnit;

import javax.ws.rs.*;

import com.ymatou.mq.rabbit.dispatcher.constants.Constants;
import com.ymatou.mq.rabbit.dispatcher.facade.DemoFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.dubbo.config.annotation.Service;


@Component("demoResource")
@Produces({"application/json; charset=UTF-8"})
@Service(protocol = "rest")
@Path("/{api:(?i:api)}")
public class DemoResourceImpl implements DemoResource {

    public static final Logger logger = LoggerFactory.getLogger(DemoResourceImpl.class);

    @Autowired
    private DemoFacade demoFacade;


    @GET
    @Path("/{sayHello:(?i:sayHello)}")
    @Override
    public String sayHello(@QueryParam("name") String name) {
        return demoFacade.sayHello(name);
    }

    @GET
    @Path("/{testShutdownGracefully:(?i:testShutdownGracefully)}")
    @Override
    public String testShutdownGracefully() {

        logger.info("------------test begin start sleep 5 seconds -----------------");

        try {
            TimeUnit.SECONDS.sleep(5);
        } catch (InterruptedException e) {
            logger.info("eception:",e);
        }

        logger.info("------------end test request success -----------------");
        return null;
    }

    @POST
    @Path("/{shutdown:(?i:shutdown)}")
    @Override
    public String shutdown() {
        Constants.ctx.close();
        return "";
    }
}

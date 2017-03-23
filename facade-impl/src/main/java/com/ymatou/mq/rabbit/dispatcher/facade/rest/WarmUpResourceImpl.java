/*
 *
 * (C) Copyright 2016 Ymatou (http://www.ymatou.com/). All rights reserved.
 *
 */
package com.ymatou.mq.rabbit.dispatcher.facade.rest;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.alibaba.dubbo.config.annotation.Service;
import com.ymatou.mq.rabbit.dispatcher.util.Utils;

/**
 * @author tuwenjie 2016年5月20日 上午10:53:18
 */
@Component("warmUpResource")
@Path("")
@Service(protocol = "rest")
public class WarmUpResourceImpl implements WarmUpResource {

    private static final Logger LOGGER = LoggerFactory.getLogger(WarmUpResourceImpl.class);

    /*
     * (non-Javadoc)
     * 
     * @see com.ymatou.tradeservice.facade.rest.WarmUpResource#warmUp()
     */
    @Override
    @GET
    @Path("/{warmup:(?i:warmup)}")
    @Produces({MediaType.TEXT_PLAIN})
    public String warmUp() {
        return "ok";
    }


    @Override
    @GET
    @Path("/{version:(?i:version)}")
    @Produces({MediaType.TEXT_PLAIN})
    public String version() {
        try {
            return new String(Files.readAllBytes(
                    Paths.get(Utils.class.getResource("/version.txt").toURI())), Charset.forName("UTF-8"));
        } catch (Exception e) {
            LOGGER.error("Failed to read version. {}", e.getMessage(), e);
            return "Failed to read version:" + e.getMessage();
        }
    }
}

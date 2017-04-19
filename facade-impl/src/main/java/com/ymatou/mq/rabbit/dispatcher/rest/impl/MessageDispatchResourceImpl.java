/*
 *
 *  (C) Copyright 2016 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.rest.impl;

import javax.ws.rs.*;

import com.ymatou.mq.rabbit.dispatcher.facade.MessageDispatchFacade;
import com.ymatou.mq.rabbit.dispatcher.facade.model.DispatchMessageReq;
import com.ymatou.mq.rabbit.dispatcher.facade.model.DispatchMessageResp;
import com.ymatou.mq.rabbit.dispatcher.rest.DispatchMessageRestReq;
import com.ymatou.mq.rabbit.dispatcher.rest.MessageDispatchResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.alibaba.dubbo.config.annotation.Service;


@Component("messageDispatchResource")
@Produces({"application/json; charset=UTF-8"})
@Service(protocol = "rest")
@Path("/{api:(?i:api)}")
public class MessageDispatchResourceImpl implements MessageDispatchResource {

    public static final Logger logger = LoggerFactory.getLogger(MessageDispatchResourceImpl.class);

    @Autowired
    private MessageDispatchFacade messageDispatchFacade;

    @POST
    @Path("/{dispatch:(?i:dispatch)}")
    @Override
    public DispatchMessageResp dispatch(DispatchMessageRestReq req){
        DispatchMessageReq dispatchMessageReq = new DispatchMessageReq();
        dispatchMessageReq.setApp(req.getAppId());
        dispatchMessageReq.setCode(req.getCode());
        dispatchMessageReq.setMsgUniqueId(req.getMsgUniqueId());
        dispatchMessageReq.setIp(req.getIp());
        dispatchMessageReq.setBody(req.getBody());
        return messageDispatchFacade.dispatch(dispatchMessageReq);
    }

}

/*
 *
 * (C) Copyright 2017 Ymatou (http://www.ymatou.com/). All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.facade.impl;



import com.alibaba.dubbo.config.annotation.Service;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.rabbit.dispatcher.facade.MessageDispatchFacade;
import com.ymatou.mq.rabbit.dispatcher.facade.model.DispatchMessageReq;
import com.ymatou.mq.rabbit.dispatcher.facade.model.DispatchMessageResp;
import com.ymatou.mq.rabbit.dispatcher.service.MessageDispatchService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;


/**
 * @author luoshiqian 2016/8/31 14:13
 */
@Service(protocol = "dubbo")
@Component
public class MessageDispatchFacadeImpl implements MessageDispatchFacade {

    @Autowired
    private MessageDispatchService messageDispatchService;

    @Override
    public DispatchMessageResp dispatch(DispatchMessageReq req){
        Message message = new Message();

        //TODO
        messageDispatchService.dispatch(message);

        DispatchMessageResp resp = new DispatchMessageResp();
        return resp;
    }
}

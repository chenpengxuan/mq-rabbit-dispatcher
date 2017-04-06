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
import com.ymatou.mq.rabbit.dispatcher.service.MessageDispatchService;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Date;

import static org.junit.Assert.fail;

/**
 * Created by zhangzhihua on 2017/3/29.
 */
public class MessageDispatchServiceTest extends BaseTest {

    @Autowired
    MessageDispatchService messageDispatchService;

    @Test
    public void testDispatch(){
        Message message = new Message();
        messageDispatchService.dispatch(message);
    }

}

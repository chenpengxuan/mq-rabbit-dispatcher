/*
 *
 *  (C) Copyright 2016 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.rest;

import com.ymatou.mq.rabbit.dispatcher.facade.model.DispatchMessageReq;
import com.ymatou.mq.rabbit.dispatcher.facade.model.DispatchMessageResp;

/**
 * 消息分发rest
 * @author luoshiqian 2016/8/31 14:12
 */
public interface MessageDispatchResource {

    /**
     * 直接调用分发接口
     * @param req
     * @return
     */
    public DispatchMessageResp dispatch(DispatchMessageReq req);

}

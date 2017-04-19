/**
 * (C) Copyright 2016 Ymatou (http://www.ymatou.com/).
 *
 * All rights reserved.
 */
package com.ymatou.mq.rabbit.dispatcher.rest;

import com.ymatou.mq.rabbit.dispatcher.facade.model.BaseRequest;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

/**
 * 分发消息请求体
 * 
 * @author wangxudong 2016年7月27日 下午6:51:48
 *
 */
public class DispatchMessageRestReq extends BaseRequest {

    private static final long serialVersionUID = 1L;

    /**
     * 消息id，若调用方不传则生成
     */
    private String id;

    /**
     * 应用Id
     */
    @NotEmpty(message = "appId not empty")
    private String appId;


    /**
     * 业务代码
     */
    @NotEmpty(message = "code not empty")
    private String code;


    /**
     * 消息Id
     */
    @NotEmpty(message = "messageId not empty")
    private String msgUniqueId;


    /**
     * 客户端Ip
     */
    private String ip;

    /**
     * 业务消息体
     */
    @NotNull(message = "body not null")
    private String body;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String getAppId() {
        return appId;
    }

    @Override
    public void setAppId(String appId) {
        this.appId = appId;
    }

    /**
     * @return the code
     */
    public String getCode() {
        return code;
    }

    /**
     * @param code the code to set
     */
    public void setCode(String code) {
        this.code = code;
    }

    /**
     * @return the msgUniqueId
     */
    public String getMsgUniqueId() {
        return msgUniqueId;
    }

    /**
     * @param msgUniqueId the msgUniqueId to set
     */
    public void setMsgUniqueId(String msgUniqueId) {
        this.msgUniqueId = msgUniqueId;
    }

    /**
     * @return the ip
     */
    public String getIp() {
        return ip;
    }

    /**
     * @param ip the ip to set
     */
    public void setIp(String ip) {
        this.ip = ip;
    }

    /**
     * @return the body
     */
    public String getBody() {
        return body;
    }

    /**
     * @param body the body to set
     */
    public void setBody(String body) {
        this.body = body;
    }

}

/**
 * (C) Copyright 2016 Ymatou (http://www.ymatou.com/).
 *
 * All rights reserved.
 */
package com.ymatou.mq.rabbit.dispatcher.facade.model;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.alibaba.fastjson.serializer.SimplePropertyPreFilter;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;

/**
 * 分发消息请求体
 * 
 * @author wangxudong 2016年7月27日 下午6:51:48
 *
 */
public class DispatchMessageReq extends BaseRequest {

    private static final long serialVersionUID = 1L;

    /**
     * 消息id，若调用方不传则生成
     */
    private String id;

    /**
     * 应用Id，因可能走dubbo原因，appId无法传递，facade接口appId改为app,rest接口仍为appId
     */
    @NotEmpty(message = "app not empty")
    private String app;


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

    public String getApp() {
        return app;
    }

    public void setApp(String app) {
        this.app = app;
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

    @Override
    public String toString() {
        SimplePropertyPreFilter filter = new SimplePropertyPreFilter();

        //body无需输出到日志
        filter.getExcludes().add("body");
        return JSON.toJSONString(this, filter, SerializerFeature.WriteDateUseDateFormat, SerializerFeature.SkipTransientField);
    }

}

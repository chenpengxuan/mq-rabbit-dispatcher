package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.model.*;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.infrastructure.service.MessageService;
import com.ymatou.mq.infrastructure.support.enums.CallbackFromEnum;
import com.ymatou.mq.infrastructure.support.enums.CompensateFromEnum;
import com.ymatou.mq.infrastructure.support.enums.CompensateStatusEnum;
import com.ymatou.mq.infrastructure.support.enums.DispatchStatusEnum;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.util.EntityUtils;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.Date;
import java.util.List;

/**
 * 消息分发callack service
 * Created by zhangzhihua on 2017/4/1.
 */
@Component
public class DispatchCallbackService {

    private static final Logger logger = LoggerFactory.getLogger(DispatchCallbackService.class);

    @Autowired
    private MessageConfigService messageConfigService;

    @Autowired
    private MessageService messageService;

    /**
     * 回调处理
     * @param message
     */
    public void invoke(Message message){
        List<CallbackConfig> callbackConfigList = messageConfigService.getCallbackConfigList(message.getAppId(),message.getQueueCode());
        if(CollectionUtils.isEmpty(callbackConfigList)){
            logger.error("appId:{},queueCode:{} not exist subscribler.",message.getAppId(),message.getQueueCode());
            return;
        }

        for(CallbackConfig callbackConfig:callbackConfigList){
            doInvokeOne(message,callbackConfig,null);
        }

    }

    /**
     * 调用一个订阅者
     * @param message
     * @param callbackConfig
     */
    void doInvokeOne(Message message,CallbackConfig callbackConfig,Long timeout){
        //async http send
        try {
            new AsyncHttpInvokeService(message,callbackConfig,this).send();
        } catch (Exception e) {
            logger.error("doInvokeOne error.",e);
        }
    }

    /**
     *
     * @param message
     * @param callbackConfig
     */
    public void onInvokeSuccess(Message message,CallbackConfig callbackConfig,HttpResponse result){
        try {
            //更新分发明细状态
            CallbackResult callbackResult = this.buildCallbackResult(message,callbackConfig,result);
            messageService.updateDispatchDetail(callbackResult);
        } catch (Exception e) {
            logger.error("onInvokeSuccess proccess error.",e);
        }
    }

    /**
     *
     * @param message
     * @param callbackConfig
     */
    public void onInvokeFail(Message message,CallbackConfig callbackConfig,Exception ex){
        try {
            boolean isNeedInsertCompensate = this.isNeedInsertCompensate(callbackConfig);
            if(isNeedInsertCompensate){//若需要插补单
                //插补单
                MessageCompensate messageCompensate = this.buildCompensate(message,callbackConfig);
                //TODO 补充补单字段
                messageService.insertCompensate(messageCompensate);

                //更新分发明细状态
                CallbackResult callbackResult = this.buildCallbackResult(message,callbackConfig,ex,true);
                messageService.updateDispatchDetail(callbackResult);
            }else{//若不需要插补单
                //更新分发明细状态
                CallbackResult callbackResult = this.buildCallbackResult(message,callbackConfig,ex,false);
                messageService.updateDispatchDetail(callbackResult);
            }
        } catch (Exception e) {
            logger.error("onInvokeFail proccess error.",e);
        }
    }

    /**
     * 判断是否需要插补单记录，开启配置&开启消息存储&开启补单
     * @param callbackConfig
     * @return
     */
    boolean isNeedInsertCompensate(CallbackConfig callbackConfig){
        //若队列配置、回调配置开启
        if(callbackConfig.getQueueConfig().getEnable() && callbackConfig.getEnable()){
            //若队列配置开启消息存储
            if(callbackConfig.getQueueConfig().getEnableLog()){
                //若开启补单
                if(callbackConfig.getIsRetry() != null && callbackConfig.getIsRetry() > 0){
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 根据返回代码判断是否成功
     * @param statusCode
     * @param body
     * @return
     */
    boolean isCallbackSuccess(int statusCode, String body) {
        if (statusCode == 200 && body != null
                && (body.trim().equalsIgnoreCase("ok") || body.trim().equalsIgnoreCase("\"ok\""))) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 构造回调结果，正常响应的情况
     * @param message
     * @param callbackConfig
     * @param result
     * @return
     */
    CallbackResult buildCallbackResult(Message message,CallbackConfig callbackConfig,HttpResponse result) throws IOException {
        HttpEntity entity = result.getEntity();
        int statusCode = result.getStatusLine().getStatusCode();
        String reponse = EntityUtils.toString(entity, "UTF-8");

        CallbackResult callbackResult = new CallbackResult();
        callbackResult.setAppId(message.getAppId());
        callbackResult.setQueueCode(message.getQueueCode());
        callbackResult.setConsumerId(callbackConfig.getCallbackKey());
        callbackResult.setMsgId(message.getId());
        callbackResult.setBizId(message.getBizId());
        //来源
        callbackResult.setFrom(CallbackFromEnum.DISPATCH.ordinal());
        //url
        callbackResult.setUrl(callbackConfig.getUrl());
        //请求报文
        callbackResult.setRequest(message.getBody());
        //响应报文
        callbackResult.setResponse(reponse);
        //请求时间
        callbackResult.setReqTime(message.getCreateTime());
        //响应时间
        callbackResult.setRespTime(new Date());
        //调用结果
        if(this.isCallbackSuccess(statusCode,reponse)){
            callbackResult.setStatus(DispatchStatusEnum.SUCCESS.ordinal());
        }else{
            callbackResult.setStatus(DispatchStatusEnum.FAIL.ordinal());
        }
        return callbackResult;
    }

    /**
     * 构造回调结果，响应异常或无响应的
     * @param message
     * @param callbackConfig
     * @param ex
     * @param isNeedCompensate
     * @return
     */
    CallbackResult buildCallbackResult(Message message,CallbackConfig callbackConfig,Exception ex,boolean isNeedCompensate){
        CallbackResult callbackResult = new CallbackResult();
        callbackResult.setAppId(message.getAppId());
        callbackResult.setQueueCode(message.getQueueCode());
        callbackResult.setConsumerId(callbackConfig.getCallbackKey());
        callbackResult.setMsgId(message.getId());
        callbackResult.setBizId(message.getBizId());
        //来源
        callbackResult.setFrom(CallbackFromEnum.DISPATCH.ordinal());
        //url
        callbackResult.setUrl(callbackConfig.getUrl());
        //请求报文
        callbackResult.setRequest(message.getBody());
        //响应报文
        callbackResult.setResponse(ex != null?ex.getLocalizedMessage():"");
        //请求时间
        callbackResult.setReqTime(message.getCreateTime());
        //响应时间
        callbackResult.setRespTime(new Date());
        //调用结果
        if(isNeedCompensate){
            callbackResult.setStatus(DispatchStatusEnum.COMPENSATE.ordinal());
        }else{
            callbackResult.setStatus(DispatchStatusEnum.FAIL.ordinal());
        }
        return callbackResult;
    }

    /**
     * 构造补单
     * @param message
     * @param callbackConfig
     * @return
     */
    MessageCompensate buildCompensate(Message message,CallbackConfig callbackConfig){
        MessageCompensate messageCompensate = new MessageCompensate();
        messageCompensate.setId(String.format("%s_%s",message.getId(),callbackConfig.getCallbackKey()));
        messageCompensate.setMsgId(message.getId());
        messageCompensate.setAppId(message.getAppId());
        messageCompensate.setQueueCode(message.getQueueCode());
        messageCompensate.setConsumerId(callbackConfig.getCallbackKey());
        messageCompensate.setBizId(message.getBizId());
        messageCompensate.setBody(message.getBody());
        messageCompensate.setSource(CompensateFromEnum.DISPATCH.ordinal());
        messageCompensate.setStatus(CompensateStatusEnum.INIT.ordinal());
        messageCompensate.setCreateTime(new Date());
        //TODO 下次补单时间
        return messageCompensate;
    }

}

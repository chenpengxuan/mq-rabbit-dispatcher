package com.ymatou.mq.rabbit.dispatcher.service;

import com.alibaba.fastjson.JSON;
import com.ymatou.mq.infrastructure.model.*;
import com.ymatou.mq.infrastructure.service.AsyncHttpInvokeService;
import com.ymatou.mq.infrastructure.service.HttpInvokeResultService;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.infrastructure.service.MessageService;
import com.ymatou.mq.infrastructure.support.enums.CallbackFromEnum;
import com.ymatou.mq.infrastructure.support.enums.CompensateFromEnum;
import com.ymatou.mq.infrastructure.support.enums.CompensateStatusEnum;
import com.ymatou.mq.infrastructure.support.enums.DispatchStatusEnum;
import com.ymatou.mq.rabbit.dispatcher.support.Action;
import com.ymatou.mq.rabbit.dispatcher.support.ActionConstants;
import com.ymatou.mq.rabbit.dispatcher.support.ActionListener;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.Date;

/**
 * 消息分发callack service
 * Created by zhangzhihua on 2017/4/1.
 */
@Component
public class DispatchCallbackService implements HttpInvokeResultService {

    private static final Logger logger = LoggerFactory.getLogger(DispatchCallbackService.class);

    @Autowired
    private MessageConfigService messageConfigService;

    @Autowired
    private MessageService messageService;

    @Autowired
    private ActionFileQueueService actionFileQueueService;

    @PostConstruct
    public void init(){
        String key = String.format("%s_%d", ActionConstants.ENTITY_DISPATCH, ActionConstants.ACTION_TYPE_UPDATE);
        actionFileQueueService.addActionListener(key,new UpdateDetailActionListener());
    }

    /**
     * 回调
     * @param callbackMessage
     */
    public void invoke(CallbackMessage callbackMessage){
        CallbackConfig callbackConfig = messageConfigService.getCallbackConfig(callbackMessage.getCallbackKey());
        if(callbackConfig == null){
            logger.error("callback config appId:{},queueCode:{},callbackKey:{} not exist.",callbackMessage.getAppId(),callbackMessage.getQueueCode(),callbackMessage.getCallbackKey());
            return;
        }

        doInvokeOne(callbackMessage,callbackConfig,null);
    }

    /**
     * 调用一个订阅者
     * @param callbackMessage
     * @param callbackConfig
     * @param timeout
     */
    void doInvokeOne(CallbackMessage callbackMessage,CallbackConfig callbackConfig,Long timeout){
        //async http send
        try {
            new AsyncHttpInvokeService(callbackMessage,callbackConfig,this).send();
        } catch (Exception e) {
            logger.error("doInvokeOne error.",e);
        }
    }

    /**
     *
     * @param message
     * @param callbackConfig
     */
    @Override
    public void onInvokeSuccess(CallbackMessage message,CallbackConfig callbackConfig){
        try {
            //更新分发明细状态
            CallbackResult callbackResult = this.buildCallbackResult(message,callbackConfig,message.getResponse());
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
    @Override
    public void onInvokeFail(CallbackMessage message,CallbackConfig callbackConfig){
        try {
            boolean isNeedInsertCompensate = this.isNeedInsertCompensate(callbackConfig);
            if(isNeedInsertCompensate){//若需要插补单
                //插补单
                MessageCompensate messageCompensate = this.buildCompensate(message,callbackConfig);
                messageService.insertCompensate(messageCompensate);

                //更新分发明细状态
                CallbackResult callbackResult = this.buildCallbackResult(message,callbackConfig,true);
                Action action = buildAction(ActionConstants.ENTITY_DISPATCH, ActionConstants.ACTION_TYPE_UPDATE,callbackResult);
                actionFileQueueService.saveActionToFileDb(action);
            }else{//若不需要插补单
                //更新分发明细状态
                CallbackResult callbackResult = this.buildCallbackResult(message,callbackConfig,false);
                Action action = buildAction(ActionConstants.ENTITY_DISPATCH, ActionConstants.ACTION_TYPE_UPDATE,callbackResult);
                actionFileQueueService.saveActionToFileDb(action);
            }
        } catch (Exception e) {
            logger.error("onInvokeFail proccess error.",e);
        }
    }

    /**
     * 构造action
     * @param callbackResult
     * @return
     */
    Action buildAction(String entity,int actionType,CallbackResult callbackResult){
        Action action = new Action();
        action.setEntity(entity);
        action.setActionType(actionType);
        action.setId(ObjectId.get().toString());
        action.setObj(callbackResult);
        return action;
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
    CallbackResult buildCallbackResult(CallbackMessage message,CallbackConfig callbackConfig,String result) throws IOException {
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
        callbackResult.setResponse(result);
        //请求时间
        callbackResult.setReqTime(message.getCreateTime());
        //响应时间
        callbackResult.setRespTime(new Date());
        //调用结果
        callbackResult.setStatus(DispatchStatusEnum.SUCCESS.ordinal());
        return callbackResult;
    }

    /**
     * 构造回调结果，响应异常或无响应的
     * @param message
     * @param callbackConfig
     * @param isNeedCompensate
     * @return
     */
    CallbackResult buildCallbackResult(CallbackMessage message,CallbackConfig callbackConfig,boolean isNeedCompensate){
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
        callbackResult.setRequest(message.getResponse());
        //响应报文
        callbackResult.setResponse(message.getResponse());
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
    MessageCompensate buildCompensate(CallbackMessage message,CallbackConfig callbackConfig){
        MessageCompensate messageCompensate = new MessageCompensate();
        messageCompensate.setId(String.format("%s_%s",message.getId(),callbackConfig.getCallbackKey()));
        messageCompensate.setMsgId(message.getId());
        messageCompensate.setAppId(message.getAppId());
        messageCompensate.setQueueCode(message.getQueueCode());
        messageCompensate.setConsumerId(callbackConfig.getCallbackKey());
        messageCompensate.setBizId(message.getBizId());
        messageCompensate.setBody(message.getBody());
        messageCompensate.setSource(CompensateFromEnum.DISPATCH.ordinal());
        messageCompensate.setStatus(CompensateStatusEnum.COMPENSATE.ordinal());
        messageCompensate.setCreateTime(new Date());
        messageCompensate.setUpdateTime(new Date());
        messageCompensate.setNextTime(new Date());
        return messageCompensate;
    }

    /**
     * 更新明细操作监听
     */
    class UpdateDetailActionListener implements ActionListener{

        @Override
        public void execute(Object obj) {
            logger.info("execute updateDetail action...");
            CallbackResult callbackResult = JSON.parseObject(String.valueOf(obj),CallbackResult.class);
            messageService.updateDispatchDetail(callbackResult);
        }
    }

}

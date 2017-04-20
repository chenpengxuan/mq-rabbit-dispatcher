package com.ymatou.mq.rabbit.dispatcher.service;

import com.alibaba.fastjson.JSON;
import com.ymatou.mq.infrastructure.model.*;
import com.ymatou.mq.infrastructure.service.AsyncHttpInvokeService;
import com.ymatou.mq.infrastructure.service.HttpInvokeResultService;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.infrastructure.service.MessageService;
import com.ymatou.mq.infrastructure.support.ErrorReportClient;
import com.ymatou.mq.infrastructure.support.enums.CallbackFromEnum;
import com.ymatou.mq.infrastructure.support.enums.CompensateFromEnum;
import com.ymatou.mq.infrastructure.support.enums.CompensateStatusEnum;
import com.ymatou.mq.infrastructure.support.enums.DispatchStatusEnum;
import com.ymatou.mq.rabbit.dispatcher.support.Action;
import com.ymatou.mq.rabbit.dispatcher.support.ActionConstants;
import com.ymatou.mq.rabbit.dispatcher.support.ActionListener;
import com.ymatou.performancemonitorclient.PerformanceStatisticContainer;
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

    public static final String MONITOR_APP_ID = "mqmonitor.iapi.ymatou.com";

    /**
     * 每个回调key url的业务性能监控
     */
    public static final String MONITOR_CALLBACK_KEY_URL_APP_ID = "mqmonitor.callbackkeyurl.iapi.ymatou.com";

    @Autowired
    private MessageConfigService messageConfigService;

    @Autowired
    private MessageService messageService;

    @Autowired
    private ActionFileQueueService actionFileQueueService;

    @Autowired
    private ErrorReportClient errorReportClient;

    @PostConstruct
    public void init(){
        //插消息事件监听
        String key = String.format("%s_%d", ActionConstants.ENTITY_MESSAGE, ActionConstants.ACTION_TYPE_INSERT);
        actionFileQueueService.addActionListener(key,new InsertMessageActionListener());

        //更新分发明细事件监听
        key = String.format("%s_%d", ActionConstants.ENTITY_DISPATCH, ActionConstants.ACTION_TYPE_UPDATE);
        actionFileQueueService.addActionListener(key,new UpdateDetailActionListener());

        //插入补单事件监听
        key = String.format("%s_%d", ActionConstants.ENTITY_COMPENSATE, ActionConstants.ACTION_TYPE_INSERT);
        actionFileQueueService.addActionListener(key,new InsertCompensateActionListener());
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

        if(callbackConfig.isDispatchEnable()){
            doInvokeOne(callbackMessage,callbackConfig,null);
        }
    }

    /**
     * 调用一个订阅者
     * @param callbackMessage
     * @param callbackConfig
     * @param timeout
     */
    void doInvokeOne(CallbackMessage callbackMessage,CallbackConfig callbackConfig,Long timeout){
        long startTime = System.currentTimeMillis();

        try {
            //async http send
            new AsyncHttpInvokeService(callbackMessage,callbackConfig,this).send();
        } catch (Exception e) {
            logger.error("doInvokeOne error.",e);
        }

        // 上报分发回调性能数据
        //FIXME: 上报的应该是callback url真正的耗时。。。
        long consumedTime = System.currentTimeMillis() - startTime;
        //FIXME: seviceid  应该是callbackurl
        PerformanceStatisticContainer.add(consumedTime, String.format("%s.dispatch", callbackConfig.getCallbackKey()),
                MONITOR_APP_ID);
    }

    /**
     *  @param callbackMessage
     * @param callbackConfig
     */
    @Override
    public void onInvokeSuccess(CallbackMessage callbackMessage, CallbackConfig callbackConfig){
        try {
            //插消息
            Message message = buildMessage(callbackMessage,callbackConfig);
            Action action = buildAction(ActionConstants.ENTITY_MESSAGE, ActionConstants.ACTION_TYPE_INSERT,message);
            actionFileQueueService.saveActionToFileDb(action);

            //更新分发明细状态
            CallbackResult callbackResult = this.buildCallbackResult(callbackMessage,callbackConfig, callbackMessage.getResponse());
            action = buildAction(ActionConstants.ENTITY_DISPATCH, ActionConstants.ACTION_TYPE_UPDATE,callbackResult);
            actionFileQueueService.saveActionToFileDb(action);
        } catch (Exception e) {
            logger.error("onInvokeSuccess proccess error.",e);
        }
    }

    /**
     *  @param callbackMessage
     * @param callbackConfig
     */
    @Override
    public void onInvokeFail(CallbackMessage callbackMessage, CallbackConfig callbackConfig){
        try {
            //插消息
            Message message = buildMessage(callbackMessage,callbackConfig);
            Action action = buildAction(ActionConstants.ENTITY_MESSAGE, ActionConstants.ACTION_TYPE_INSERT,message);
            actionFileQueueService.saveActionToFileDb(action);

            if(callbackConfig.isCompensateEnable()){//若需要插补单
                //插补单
                MessageCompensate messageCompensate = this.buildCompensate(callbackMessage,callbackConfig);
                action = buildAction(ActionConstants.ENTITY_COMPENSATE, ActionConstants.ACTION_TYPE_INSERT,messageCompensate);
                actionFileQueueService.saveActionToFileDb(action);

                //更新分发明细状态
                CallbackResult callbackResult = this.buildCallbackResult(callbackMessage,callbackConfig,true);
                action = buildAction(ActionConstants.ENTITY_DISPATCH, ActionConstants.ACTION_TYPE_UPDATE,callbackResult);
                actionFileQueueService.saveActionToFileDb(action);
            }else{//若不需要插补单
                //更新分发明细状态
                CallbackResult callbackResult = this.buildCallbackResult(callbackMessage,callbackConfig,false);
                action = buildAction(ActionConstants.ENTITY_DISPATCH, ActionConstants.ACTION_TYPE_UPDATE,callbackResult);
                actionFileQueueService.saveActionToFileDb(action);
            }
        } catch (Exception e) {
            logger.error("onInvokeFail proccess error.",e);
        } finally {
            errorReportClient.sendErrorReport(callbackMessage,callbackConfig);
        }
    }

    /**
     * 构造message
     * @param callbackMessage
     * @param callbackConfig
     * @return
     */
    Message buildMessage(CallbackMessage callbackMessage,CallbackConfig callbackConfig){
        Message message = new Message();
        message.setAppId(callbackMessage.getAppId());
        message.setQueueCode(callbackMessage.getQueueCode());
        message.setId(callbackMessage.getId());
        message.setBizId(callbackMessage.getBizId());
        message.setBody(callbackMessage.getBody());
        message.setClientIp(callbackMessage.getClientIp());
        message.setRecvIp(callbackMessage.getRecvIp());
        message.setCreateTime(callbackMessage.getCreateTime());
        return message;
    }

    /**
     * 构造action
     * @param obj
     * @return
     */
    Action buildAction(String entity, int actionType, Object obj){
        Action action = new Action();
        action.setEntity(entity);
        action.setActionType(actionType);
        action.setId(ObjectId.get().toString());
        action.setObj(obj);
        return action;
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
     * @param callbackMessage
     * @param callbackConfig
     * @param result
     * @return
     */
    CallbackResult buildCallbackResult(CallbackMessage callbackMessage, CallbackConfig callbackConfig, String result) throws IOException {
        CallbackResult callbackResult = new CallbackResult();
        callbackResult.setAppId(callbackMessage.getAppId());
        callbackResult.setQueueCode(callbackMessage.getQueueCode());
        callbackResult.setConsumerId(callbackConfig.getCallbackKey());
        callbackResult.setMsgId(callbackMessage.getId());
        callbackResult.setBizId(callbackMessage.getBizId());
        //来源
        callbackResult.setFrom(CallbackFromEnum.DISPATCH.getCode());
        //url
        callbackResult.setUrl(callbackConfig.getUrl());
        //请求报文
        callbackResult.setRequest(callbackMessage.getBody());
        //响应报文
        callbackResult.setResponse(result);
        //请求时间
        callbackResult.setReqTime(callbackMessage.getRequestTime());
        //响应时间
        callbackResult.setRespTime(new Date());
        //调用结果
        callbackResult.setStatus(DispatchStatusEnum.SUCCESS.getCode());
        return callbackResult;
    }

    /**
     * 构造回调结果，响应异常或无响应的
     * @param callbackMessage
     * @param callbackConfig
     * @param isNeedCompensate
     * @return
     */
    CallbackResult buildCallbackResult(CallbackMessage callbackMessage, CallbackConfig callbackConfig, boolean isNeedCompensate){
        CallbackResult callbackResult = new CallbackResult();
        callbackResult.setAppId(callbackMessage.getAppId());
        callbackResult.setQueueCode(callbackMessage.getQueueCode());
        callbackResult.setConsumerId(callbackConfig.getCallbackKey());
        callbackResult.setMsgId(callbackMessage.getId());
        callbackResult.setBizId(callbackMessage.getBizId());
        //来源
        callbackResult.setFrom(CallbackFromEnum.DISPATCH.getCode());
        //url
        callbackResult.setUrl(callbackConfig.getUrl());
        //请求报文
        callbackResult.setRequest(callbackMessage.getResponse());
        //响应报文
        callbackResult.setResponse(callbackMessage.getResponse());
        //请求时间
        callbackResult.setReqTime(callbackMessage.getRequestTime());
        //响应时间
        callbackResult.setRespTime(new Date());
        //调用结果
        if(isNeedCompensate){
            callbackResult.setStatus(DispatchStatusEnum.COMPENSATE.getCode());
        }else{
            callbackResult.setStatus(DispatchStatusEnum.FAIL.getCode());
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
        messageCompensate.setSource(CompensateFromEnum.DISPATCH.getCode());
        messageCompensate.setStatus(CompensateStatusEnum.COMPENSATE.getCode());
        messageCompensate.setCreateTime(new Date());
        messageCompensate.setUpdateTime(new Date());
        messageCompensate.setNextTime(new Date());
        return messageCompensate;
    }

    /**
     * 插入消息操作监听
     */
    class InsertMessageActionListener implements ActionListener{

        @Override
        public void execute(Object obj) {
            logger.debug("execute insertMessage action...");
            Message message = JSON.parseObject(String.valueOf(obj),Message.class);
            messageService.saveMessage(message);
        }
    }

    /**
     * 更新明细操作监听
     */
    class UpdateDetailActionListener implements ActionListener{

        @Override
        public void execute(Object obj) {
            logger.debug("execute updateDetail action...");
            CallbackResult callbackResult = JSON.parseObject(String.valueOf(obj),CallbackResult.class);
            messageService.updateDispatchDetail(callbackResult);
        }
    }

    /**
     * 插入补单
     */
    class InsertCompensateActionListener implements ActionListener{

        @Override
        public void execute(Object obj) {
            logger.debug("execute insertCompensate action...");
            MessageCompensate messageCompensate = JSON.parseObject(String.valueOf(obj),MessageCompensate.class);
            messageService.insertCompensate(messageCompensate);
        }
    }

}

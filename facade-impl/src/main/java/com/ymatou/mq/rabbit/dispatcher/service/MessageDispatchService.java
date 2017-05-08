package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.model.CallbackConfig;
import com.ymatou.mq.infrastructure.model.CallbackMessage;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.infrastructure.model.QueueConfig;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.infrastructure.service.MessageService;
import com.ymatou.mq.infrastructure.support.SemaphorManager;
import com.ymatou.mq.rabbit.dispatcher.facade.model.BizException;
import com.ymatou.mq.rabbit.dispatcher.facade.model.ErrorCode;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Date;
import java.util.Map;

/**
 * message dispatch分发service
 * Created by zhangzhihua on 2017/4/1.
 */
@Component
public class MessageDispatchService{

    private static final Logger logger = LoggerFactory.getLogger(MessageDispatchService.class);

    @Autowired
    private MessageFileQueueService messageFileQueueService;

    @Autowired
    private MessageService messageService;

    @Autowired
    private MessageConfigService messageConfigService;

    @Autowired
    private DispatchCallbackService dispatchCallbackService;

    /**
     * 由接收站直接调用的分发处理接口
     * @param message
     */
    public boolean dispatch(Message message){
        logger.info("direct dispatch message:{}.",message);
        //验证队列有效性
        this.validQueue(message.getAppId(),message.getQueueCode());

        //写fileDb
        boolean result = messageFileQueueService.saveMessageToFileDb(message);
        //若写失败，则同步写mongo或直接分发
        if(!result){
            return saveAndDispatchMessage(message);
        }
        return result;
    }

    /**
     * 保存消息到mongo并直接分发
     * @param message
     * @return
     */
    boolean saveAndDispatchMessage(Message message){
        boolean saveMsgResult = false;
        boolean dispatchResult = false;

        //写消息
        try {
            messageService.saveMessage(message);
            saveMsgResult = true;
        } catch (Exception e) {
            logger.error("save message:{} to mongo error",message,e);
        }

        //分发回调
        try {
            QueueConfig queueConfig = messageConfigService.getQueueConfig(message.getAppId(),message.getQueueCode());
            if ( queueConfig != null ) {
                for (CallbackConfig callbackConfig : queueConfig.getCallbackCfgList()) {
                    //未开启则跳过
                    if (!queueConfig.getEnable() || !callbackConfig.getEnable()) {
                        continue;
                    }
                    dispatchCallbackService.invoke(toCallbackMessage(message, callbackConfig.getCallbackKey()));
                }
            }
            dispatchResult = true;
        } catch (Exception e) {
            logger.error("dispatch message:{} error", message,e);
        }

        //若写消息或分发有一个成功，则返回true
        if(dispatchResult || saveMsgResult){
            return true;
        }else{
            return false;
        }
    }

    /**
     * 转化为CallbackMessage
     * @param message
     * @return
     */
    CallbackMessage toCallbackMessage(Message message, String callbackKey){
        CallbackMessage callbackMessage = new CallbackMessage();
        callbackMessage.setAppId(message.getAppId());
        callbackMessage.setQueueCode(message.getQueueCode());
        callbackMessage.setCallbackKey(callbackKey);
        callbackMessage.setId(message.getId());
        callbackMessage.setBizId(message.getBizId());
        callbackMessage.setBody(message.getBody());
        callbackMessage.setClientIp(message.getClientIp());
        callbackMessage.setRecvIp(message.getRecvIp());
        callbackMessage.setCreateTime(message.getCreateTime() != null?message.getCreateTime():new Date());
        return callbackMessage;
    }

    /**
     * 验证queuCode有效性
     */
    void validQueue(String appId,String queueCode){
        QueueConfig queueConfig = messageConfigService.getQueueConfig(appId, queueCode);
        if(queueConfig == null){
            throw new BizException(ErrorCode.QUEUE_CONFIG_NOT_EXIST,String.format("appId:[%s],queueCode:[%s] not exist.",appId, queueCode));
        }
        if(!queueConfig.getEnable()){
            throw new BizException(ErrorCode.QUEUE_CONFIG_NOT_ENABLE,String.format("appId:[%s],queueCode:[%s] not enabled.",appId, queueCode));
        }
    }

}

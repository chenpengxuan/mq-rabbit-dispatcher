package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.model.CallbackConfig;
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
        //若写失败，则同步写mongo
        if(!result){
            try {
                return messageService.saveMessage(message);
            } catch (Exception e) {
                logger.error("save message to mongo error.",e);
                return false;
            }
        }
        return result;
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

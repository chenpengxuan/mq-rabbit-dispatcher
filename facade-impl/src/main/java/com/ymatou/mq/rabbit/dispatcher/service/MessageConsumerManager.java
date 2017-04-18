package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.model.AppConfig;
import com.ymatou.mq.infrastructure.model.CallbackConfig;
import com.ymatou.mq.infrastructure.model.QueueConfig;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.infrastructure.support.ConfigReloadListener;
import com.ymatou.mq.infrastructure.support.SemaphorManager;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.dispatcher.config.DispatchConfig;

import com.ymatou.mq.rabbit.support.RabbitConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MQ消息消费管理，如启动、关闭消费等
 * Created by zhangzhihua on 2017/4/5.
 */
@Component
public class MessageConsumerManager {

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumerManager.class);

    @Autowired
    private MessageConfigService messageConfigService;

    @Autowired
    private DispatchConfig dispatchConfig;

    @Autowired
    private RabbitConfig rabbitConfig;

    @Autowired
    private DispatchCallbackService dispatchCallbackService;

    /**
     * messageConumser映射表
     */
    private static Map<String,MessageConsumer> messageConsumerMap = new ConcurrentHashMap<String,MessageConsumer>();

    /**
     * 启动消费监听
     */
    @PostConstruct
    public void startAll(){
        //启动消费监听
        String groupId = dispatchConfig.getGroupId();
        logger.debug("current group id:{}.",groupId);
        List<AppConfig> appConfigList = messageConfigService.getAllAppConfig();
        logger.debug("appConfigList size:{}.",appConfigList != null?appConfigList.size():"0");
        String[] clusters = {RabbitConstants.CLUSTER_MASTER,RabbitConstants.CLUSTER_SLAVE};
        for(AppConfig appConfig:appConfigList){
            String dispatchGroup = appConfig.getDispatchGroup();
            //若所属分组为空或者不匹配，则跳过
            if (dispatchGroup == null || !dispatchGroup.contains(groupId)) {
                continue;
            }
            for (QueueConfig queueConfig : appConfig.getMessageCfgList()) {
                for(CallbackConfig callbackConfig:queueConfig.getCallbackCfgList()){
                    //若配置没有开启，则跳过
                    if(!queueConfig.getEnable() || !callbackConfig.getEnable()){
                        continue;
                    }
                    for(String cluster:clusters){
                        startConsumer(appConfig.getAppId(),queueConfig.getCode(),callbackConfig.getCallbackKey(),cluster);
                    }
                }
            }
        }

    }

    /**
     * 启动consumer
     * @param appId
     * @param queueCode
     * @param callbackKey
     * @param cluster
     */
    void startConsumer(String appId,String queueCode,String callbackKey,String cluster){
        try {
            MessageConsumer messageConsumer = new MessageConsumer(appId,queueCode,callbackKey,cluster);
            messageConsumer.setRabbitConfig(rabbitConfig);
            messageConsumer.setDispatchCallbackService(dispatchCallbackService);
            messageConsumer.start();

            //添加到消费映射表
            String messageConsumerId = String.format("%s_%s",callbackKey,cluster);
            if(messageConsumerMap.get(messageConsumerId) == null){
                messageConsumerMap.put(messageConsumerId,messageConsumer);
            }
        } catch (Exception e) {
            logger.error("start consumer appId:{},queueCode:{},callbackKey:{},cluster:{} error.",appId,queueCode,callbackKey,cluster,e);
        }
    }

    /**
     * 停止所有queue监听
     */
    public void stopAll(){
        //关停消费监听
        for(MessageConsumer messageConsumer:messageConsumerMap.values()){
            messageConsumer.stop();
            messageConsumerMap.remove(messageConsumer);
        }
    }

    /**
     * stop对应的消费监听
     * @param callbackKey
     * @param cluster
     */
    public void stopConsumer(String callbackKey,String cluster){
        try {
            String messageConsumerId = String.format("%s_%s",callbackKey,cluster);
            MessageConsumer messageConsumer = messageConsumerMap.get(messageConsumerId);
            messageConsumer.stop();
            messageConsumerMap.remove(messageConsumerId);
        } catch (Exception e) {
            logger.error("stop consumer callbackKey:{},cluster:{} error.",callbackKey,cluster,e);
        }
    }

    public static Map<String, MessageConsumer> getMessageConsumerMap() {
        return messageConsumerMap;
    }

}

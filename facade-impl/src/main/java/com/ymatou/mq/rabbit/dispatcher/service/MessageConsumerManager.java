package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.model.AppConfig;
import com.ymatou.mq.infrastructure.model.CallbackConfig;
import com.ymatou.mq.infrastructure.model.QueueConfig;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.infrastructure.support.ConfigReloadListener;
import com.ymatou.mq.infrastructure.support.SemaphorManager;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.dispatcher.config.DispatchConfig;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import java.util.*;

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
     * 启动消费监听
     */
    @PostConstruct
    public void startAll(){
        //初始化信号量
        Map<String, CallbackConfig> callbackConfigMap = messageConfigService.getCallbackConfigMap();
        SemaphorManager.initSemaphores(callbackConfigMap.values());

        //添加配置变化监听
        messageConfigService.addConfigCacheListener(new ConfigReloadListener(){
            @Override
            public void callback() {
                //TODO 处理配置变化
            }
        });

        //启动消费监听
        String groupId = dispatchConfig.getGroupId();
        logger.debug("current group id:{}.",groupId);
        List<AppConfig> appConfigList = messageConfigService.getAllAppConfig();
        logger.debug("appConfigList size:{}.",appConfigList != null?appConfigList.size():"0");
        for(AppConfig appConfig:appConfigList){
            String dispatchGroup = appConfig.getDispatchGroup();
            if (dispatchGroup != null && dispatchGroup.contains(groupId)) {
                for (QueueConfig queueConfig : appConfig.getMessageCfgList()) {
                    for(CallbackConfig callbackConfig:queueConfig.getCallbackCfgList()){
                        MessageConsumer messageConsumer = new MessageConsumer(appConfig.getAppId(),queueConfig.getCode(),callbackConfig.getCallbackKey());
                        messageConsumer.setRabbitConfig(rabbitConfig);
                        messageConsumer.setDispatchCallbackService(dispatchCallbackService);
                        messageConsumer.start();
                    }
                }
            }else{
                //TODO 处理需要关停
            }
        }

    }

    /**
     * 停止所有queue监听
     */
    public void stopAll(){
        //TODO 停止所有queue
    }

    /**
     * stop对应的queue消费监听
     * @param queueCode
     */
    public void stop(String queueCode){
        //TODO
    }
}

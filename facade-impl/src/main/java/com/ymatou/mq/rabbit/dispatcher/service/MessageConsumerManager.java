package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.model.AppConfig;
import com.ymatou.mq.infrastructure.model.QueueConfig;
import com.ymatou.mq.infrastructure.service.MessageConfigService;
import com.ymatou.mq.infrastructure.support.ConfigReloadListener;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.dispatcher.config.DispatchConfig;
import org.springframework.beans.factory.annotation.Autowired;

import javax.annotation.PostConstruct;
import java.util.List;

/**
 * MQ消息消费管理，如启动、关闭消费等
 * Created by zhangzhihua on 2017/4/5.
 */
public class MessageConsumerManager {

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
        //TODO 添加配置变化监听
        messageConfigService.addConfigCacheListener(new ConfigReloadListener(){
            @Override
            public void callback() {
                //TODO
            }
        });
        //启动消费监听
        List<AppConfig> appConfigList = messageConfigService.getAllAppConfig();
        for(AppConfig appConfig:appConfigList){
            String dispatchGroup = appConfig.getDispatchGroup();
            if (dispatchGroup != null && dispatchGroup.contains(dispatchConfig.getGroupId())) {
                for (QueueConfig queueConfig : appConfig.getMessageCfgList()) {
                    MessageConsumer messageConsumer = new MessageConsumer(appConfig.getAppId(),queueConfig.getCode());
                    messageConsumer.setRabbitConfig(rabbitConfig);
                    messageConsumer.setDispatchCallbackService(dispatchCallbackService);
                    messageConsumer.start();
                }
            }else{
                //TODO
            }
        }
    }
}

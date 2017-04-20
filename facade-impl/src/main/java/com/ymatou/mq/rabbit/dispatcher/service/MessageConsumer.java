package com.ymatou.mq.rabbit.dispatcher.service;

import com.rabbitmq.client.*;
import com.ymatou.mq.infrastructure.model.CallbackMessage;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.rabbit.RabbitChannelFactory;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.ChannelWrapper;
import com.ymatou.mq.rabbit.support.RabbitConstants;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * rabbit consumer
 * Created by zhangzhihua on 2017/4/1.
 */
public class MessageConsumer implements Consumer{

    private static final Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

    /**
     * 应用id
     */
    private String appId;

    /**
     * 队列code
     */
    private String queueCode;

    /**
     * 回调url KEY
     */
    private String callbackKey;

    /**
     * 集群名称
     */
    private String cluster;

    /**
     * 集群通道
     */
    private Channel channel;

    /**
     * rabbit配置
     */
    private RabbitConfig rabbitConfig;

    /**
     * dispatch回调服务
     */
    private DispatchCallbackService dispatchCallbackService;

    public MessageConsumer(String appId, String queueCode,String callbackKey,String cluster){
        this.appId = appId;
        this.queueCode = queueCode;
        this.callbackKey = callbackKey;
        this.cluster = cluster;
    }

    /**
     * 启动消费监听
     */
    public void start() throws IOException {
        //TODO 可调整conn/channel对应的数量关系
        ChannelWrapper masterChannelWrapper = RabbitChannelFactory.createChannelWrapper(cluster,rabbitConfig);
        channel = masterChannelWrapper.getChannel();
        //TODO 处理channel关闭事件
        //FIXME:不需要吧
        channel.addShutdownListener(new ShutdownListener() {
            @Override
            public void shutdownCompleted(ShutdownSignalException cause) {
                logger.error("One rabbitmq channel shutdownCompleted ", cause);
            }
        });
        channel.basicConsume(callbackKey,false,this);
    }

    /**
     * 关闭消费监听
     */
    public void stop(){
        logger.info("close message consumer callbackKey:{},cluster:{}",callbackKey,cluster);
        if(channel != null && channel.isOpen()){
            try {
                channel.close();
            } catch (Exception e) {
                logger.error("close channel error.",e);
            }
        }
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        //FIXME: debug级别，否则日志太多。。。。
        logger.info("consumerTag:{},envelope:{},properties:{}.",consumerTag,envelope,properties);

        try {
            CallbackMessage callbackMessage = new CallbackMessage();
            callbackMessage.setAppId(appId);
            callbackMessage.setQueueCode(queueCode);
            callbackMessage.setCallbackKey(callbackKey);
            String msgId = properties.getMessageId();
            callbackMessage.setId(msgId);
            String bizId = properties.getCorrelationId();
            callbackMessage.setBizId(bizId);
            //FIXME:编码格式要跟receiver一致，都明确设为"UTF-8"??
            String sbody = (String) SerializationUtils.deserialize(body);
            callbackMessage.setBody(sbody);

            dispatchCallbackService.invoke(callbackMessage);
        } catch (Exception e) {
            logger.error("handleDelivery message error,consumerTag:{},envelope:{},properties:{}.",consumerTag,envelope,properties,e);
        } finally {
            //TODO 更新消息状态为consumed
            channel.basicAck(envelope.getDeliveryTag(),false);
        }
    }

    @Override
    public void handleCancel(String consumerTag) throws IOException {

    }

    @Override
    public void handleConsumeOk(String consumerTag) {

    }

    @Override
    public void handleCancelOk(String consumerTag) {

    }

    @Override
    public void handleShutdownSignal(String consumerTag, ShutdownSignalException sig) {

    }

    @Override
    public void handleRecoverOk(String consumerTag) {

    }

    public RabbitConfig getRabbitConfig() {
        return rabbitConfig;
    }

    public String getQueueCode() {
        return queueCode;
    }

    public void setQueueCode(String queueCode) {
        this.queueCode = queueCode;
    }

    public void setRabbitConfig(RabbitConfig rabbitConfig) {
        this.rabbitConfig = rabbitConfig;
    }

    public DispatchCallbackService getDispatchCallbackService() {
        return dispatchCallbackService;
    }

    public void setDispatchCallbackService(DispatchCallbackService dispatchCallbackService) {
        this.dispatchCallbackService = dispatchCallbackService;
    }
}

package com.ymatou.mq.rabbit.dispatcher.service;

import com.alibaba.fastjson.JSON;
import com.rabbitmq.client.*;
import com.ymatou.mq.infrastructure.model.CallbackMessage;
import com.ymatou.mq.infrastructure.model.Message;
import com.ymatou.mq.rabbit.RabbitChannelFactory;
import com.ymatou.mq.rabbit.config.RabbitConfig;
import com.ymatou.mq.rabbit.support.ChannelWrapper;
import org.apache.commons.lang.SerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
     * channel wrapper
     */
    private ChannelWrapper channelWrapper;

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
        ChannelWrapper channelWrapper = RabbitChannelFactory.createChannelWrapper(cluster,rabbitConfig);
        this.channelWrapper = channelWrapper;
        channel = channelWrapper.getChannel();
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
            RabbitChannelFactory.releaseChannelWrapper(cluster,channelWrapper);
        }
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        logger.debug("consumerTag:{},envelope:{},properties:{}.",consumerTag,envelope,properties);

        try {
            //TODO 设置编码
            Message message = (Message) SerializationUtils.deserialize(body);
            //Message message = (Message) JSON.parse(body,null);
            CallbackMessage callbackMessage = toCallbackMessage(message);
            dispatchCallbackService.invoke(callbackMessage);
        } catch (Exception e) {
            logger.error("handleDelivery message error,consumerTag:{},envelope:{},properties:{}.",consumerTag,envelope,properties,e);
        } finally {
            channel.basicAck(envelope.getDeliveryTag(),false);
        }
    }

    /**
     * 转化为callback message
     * @param message
     * @return
     */
    CallbackMessage toCallbackMessage(Message message){
        CallbackMessage callbackMessage = new CallbackMessage();
        callbackMessage.setAppId(appId);
        callbackMessage.setQueueCode(queueCode);
        callbackMessage.setCallbackKey(callbackKey);
        callbackMessage.setId(message.getId());
        callbackMessage.setBizId(message.getBizId());
        callbackMessage.setBody(message.getBody());
        callbackMessage.setClientIp(message.getClientIp());
        callbackMessage.setRecvIp(message.getRecvIp());
        return callbackMessage;
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

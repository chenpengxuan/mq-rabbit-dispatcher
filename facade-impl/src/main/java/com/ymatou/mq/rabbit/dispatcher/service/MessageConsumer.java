package com.ymatou.mq.rabbit.dispatcher.service;

import com.rabbitmq.client.*;
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
     * master集群通道
     */
    private Channel masterChannel;

    /**
     * slave集群通道
     */
    private Channel slaveChannel;

    /**
     * rabbit配置
     */
    private RabbitConfig rabbitConfig;

    /**
     * dispatch回调服务
     */
    private DispatchCallbackService dispatchCallbackService;

    public MessageConsumer(String appId, String queueCode){
        this.appId = appId;
        this.queueCode = queueCode;
    }

    /**
     * 启动消费监听
     */
    public void start(){
        try {
            //创建conn指定线程池数量
            //TODO 创建channel 不绑定线程
            //TODO 可调整conn/channel对应的数量关系
            ChannelWrapper masterChannelWrapper = RabbitChannelFactory.createChannelWrapper(rabbitConfig);
            masterChannel = masterChannelWrapper.getChannel();

            //TODO 处理channel关闭事件
            masterChannel.addShutdownListener(new ShutdownListener() {
                @Override
                public void shutdownCompleted(ShutdownSignalException cause) {
                    logger.error("shutdownCompleted,cause:" + cause);
                }
            });

            ChannelWrapper slaveChannelWrapper = RabbitChannelFactory.createChannelWrapper(rabbitConfig);
            slaveChannel = slaveChannelWrapper.getChannel();

            //ConumserHandler conumserHandler = new ConumserHandler();
            //TODO 设置ack为false，处理deliveryTag异常问题
            masterChannel.basicConsume(this.queueCode,true,this);
            slaveChannel.basicConsume(this.queueCode,true,this);
        } catch (IOException e) {
            logger.error("basic consume error,queueCode:{}.",queueCode);
        }

    }

    /**
     * 关闭消费监听
     */
    public void stop(){
        //TODO
    }

    @Override
    public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
        logger.info("consumerTag:{},envelope:{},properties:{}.",consumerTag,envelope,properties);

        //获取消息来源cluster
        String cluster = properties.getType();

        try {
            Message message = new Message();
            message.setAppId(appId);
            message.setQueueCode(queueCode);
            String sbody = (String) SerializationUtils.deserialize(body);
            message.setBody(sbody);
            String msgId = properties.getMessageId();
            message.setId(msgId);
            String bizId = properties.getCorrelationId();
            message.setBizId(bizId);

            MDC.put("logPrefix", "MessageConsumer|" + bizId);

            dispatchCallbackService.invoke(message);
        } catch (Exception e) {
            logger.error("dispatch callback error,consumerTag:{},envelope:{},properties:{}.",consumerTag,envelope,properties,e);
        } finally {
            //TODO 处理ack deliveryTag异常问题
//            if(RabbitConstants.CLUSTER_MASTER.equals(cluster)){
//                //TODO 更新消息状态为consumed
//                logger.info("masterChannel status:{}",masterChannel.isOpen());
//                masterChannel.basicAck(envelope.getDeliveryTag(),false);
//            }else{
//                slaveChannel.basicAck(envelope.getDeliveryTag(),false);
//            }
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

    public Channel getMasterChannel() {
        return masterChannel;
    }

    public void setMasterChannel(Channel masterChannel) {
        this.masterChannel = masterChannel;
    }

    public String getQueueCode() {
        return queueCode;
    }

    public void setQueueCode(String queueCode) {
        this.queueCode = queueCode;
    }

    public Channel getSlaveChannel() {
        return slaveChannel;
    }

    public void setSlaveChannel(Channel slaveChannel) {
        this.slaveChannel = slaveChannel;
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

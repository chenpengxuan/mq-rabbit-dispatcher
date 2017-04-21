package com.ymatou.mq.rabbit.dispatcher.service;

import com.ymatou.mq.infrastructure.filedb.FileDb;
import com.ymatou.mq.infrastructure.filedb.FileDbConfig;
import com.ymatou.mq.infrastructure.filedb.PutExceptionHandler;
import com.ymatou.mq.rabbit.dispatcher.config.FileDbConf;
import com.ymatou.mq.rabbit.dispatcher.support.Action;
import com.ymatou.mq.rabbit.dispatcher.support.ActionListener;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

/**
 * 操作指令文件列表处理service
 * Created by zhangzhihua on 2017/3/24.
 */
@Component
public class ActionFileQueueService implements Function<Pair<String, String>, Boolean>, PutExceptionHandler {

    private static final Logger logger = LoggerFactory.getLogger(ActionFileQueueService.class);

    private FileDb fileDb;

    @Autowired
    private FileDbConf fileDbConf;

    private static Map<String,ActionListener> actionListenerMap = new ConcurrentHashMap<String,ActionListener>();

    public FileDb getFileDb() {
        return fileDb;
    }

    public void setFileDb(FileDb fileDb) {
        this.fileDb = fileDb;
    }

    @PostConstruct
    public void init() {
        FileDbConfig fileDbConfig = FileDbConfig.newInstance()
                .setDbName(this.fileDbConf.getActionDbName())
                .setDbPath(this.fileDbConf.getActionDbPath())
                .setConsumerThreadNums(this.fileDbConf.getActionDbConsumerThreadNums())
                .setConsumeDuration(this.fileDbConf.getActionDbConsumeDuration())
                .setMaxConsumeSizeInDuration(this.fileDbConf.getActionDbMaxConsumeSizeInDuration())
                .setConsumer(this)
                .setPutExceptionHandler(this);

        fileDb = FileDb.newFileDb(fileDbConfig);
    }


    /**
     * 保存指令到文件队列
     * @param action
     */
    public void saveActionToFileDb(Action action) {
        logger.debug("save action:{} to fileDb.",action);
        fileDb.put(action.getId(), Action.toJsonString(action));
    }

    /**
     * 消费从文件获取到的数据 入库成功 返回true
     * 
     * @param pair
     * @return
     */
    @Override
    public Boolean apply(Pair<String, String> pair) {
        try {
            //获取指令
            Action action = Action.fromJson(pair.getValue());
            logger.info("consume action from fileDb,action:{}.",action);
            //执行指令操作
            processAction(action);
        } catch (Exception e) {
            logger.error("actionListener execute error.",e);
            return false;
        }
        return true;
    }

    /**
     * 执行指令对应操作
     * @param action
     */
    void processAction(Action action) throws Exception{
        ActionListener actionListener = getActionListener(String.format("%s",action.getActionType()));
        if(actionListener == null){
            throw new Exception(String.format("actionListener actionType:%s not exist.",action.getActionType()));
        }
        actionListener.execute(action.getParam());
    }

    /**
     * 添加监听
     * @param key
     * @param actionListener
     */
    public void addActionListener(String key,ActionListener actionListener){
        if(StringUtils.isBlank(key) || actionListener == null){
            throw new RuntimeException("key or actionListener not allow blank.");
        }
        if(!actionListenerMap.containsKey(key)){
            actionListenerMap.put(key,actionListener);
        }
    }

    /**
     * 获取监听
     * @return
     */
    ActionListener getActionListener(String key){
        return actionListenerMap.get(key);
    }

    @Override
    public void handleException(String key, String value, Optional<Throwable> throwable) {
        logger.error("key:{},value:{} can not save to filedb ", key, value,
                throwable.isPresent() ? throwable.get() : "");
        //若写文件队列出错，则直接执行操作
        try {
            Action action = Action.fromJson(value);
            processAction(action);
        } catch (Exception e) {
            logger.error("handleException,execute action error.",e);
        }
    }

    @PreDestroy
    public void destroy(){
        fileDb.close();
    }
}

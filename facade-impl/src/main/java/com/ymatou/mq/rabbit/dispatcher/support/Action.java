package com.ymatou.mq.rabbit.dispatcher.support;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import com.ymatou.mq.infrastructure.model.PrintFriendliness;

/**
 * 操作指令
 * Created by zhangzhihua on 2017/4/18.
 */
public class Action extends PrintFriendliness {

    /**
     * 唯一id
     */
    private String id;

    /**
     * 操作类型
     */
    private int actionType;

    /**
     * 操作对象
     */
    private Object param;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getActionType() {
        return actionType;
    }

    public void setActionType(int actionType) {
        this.actionType = actionType;
    }

    public Object getParam() {
        return param;
    }

    public void setParam(Object param) {
        this.param = param;
    }

    public static String toJsonString(Action action) {
        return JSON.toJSONString(action, SerializerFeature.SkipTransientField);
    }

    public static Action fromJson(String action) {
        return JSON.parseObject(action, Action.class);
    }

}

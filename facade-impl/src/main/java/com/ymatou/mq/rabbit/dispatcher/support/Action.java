package com.ymatou.mq.rabbit.dispatcher.support;

import com.alibaba.fastjson.JSON;
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
     * 实体名称
     */
    private String entity;

    /**
     * 操作类型 0:不指定 1:添加 2:更新 3:删除
     */
    private int actionType;

    /**
     * 操作对象
     */
    private Object obj;

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

    public Object getObj() {
        return obj;
    }

    public void setObj(Object obj) {
        this.obj = obj;
    }

    public String getEntity() {
        return entity;
    }

    public void setEntity(String entity) {
        this.entity = entity;
    }

    public static String toJsonString(Action action) {
        return JSON.toJSONString(action);
    }

    public static Action fromJson(String action) {
        return JSON.parseObject(action, Action.class);
    }

}

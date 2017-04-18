package com.ymatou.mq.rabbit.dispatcher.support;

/**
 * Action常量定义
 * @author zhangyifan 2016/9/1 12:00
 */
public class ActionConstants {

    /**
     * 实体类型-分发明细
     */
    public static final String ENTITY_DISPATCH = "dispatch";
    /**
     * 实体类型-补单
     */
    public static final String ENTITY_COMPENSATE = "compensate";


    /**
     * 操作类型-未指定
     */
    public static final int ACTION_TYPE_UNDEFINED = 0;
    /**
     * 操作类型-添加
     */
    public static final int ACTION_TYPE_ADD = 1;
    /**
     * 操作类型-更新
     */
    public static final int ACTION_TYPE_UPDATE = 2;
    /**
     * 操作类型-删除
     */
    public static final int ACTION_TYPE_DELETE = 3;
}


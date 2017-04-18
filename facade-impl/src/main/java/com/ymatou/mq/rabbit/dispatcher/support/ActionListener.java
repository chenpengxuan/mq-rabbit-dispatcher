package com.ymatou.mq.rabbit.dispatcher.support;

/**
 * action listener
 * Created by zhangzhihua on 2017/4/18.
 */
public interface ActionListener {

    /**
     * 指令操作
     * @param obj
     */
    public void execute(Object obj);
}

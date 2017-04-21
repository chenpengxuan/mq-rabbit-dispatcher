/*
 *
 *  (C) Copyright 2017 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

package com.ymatou.mq.rabbit.dispatcher.config;

import com.ymatou.performancemonitorclient.PerformanceMonitorAdvice;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 性能监控配置
 *
 * @author luoshiqian
 */
@Aspect
@Configuration
public class PerformanceConfig {

    @Value("${performance.server.url}")
    private String performanceServerUrl;


    @Bean(name = "performanceMonitorAdvice")
    public PerformanceMonitorAdvice performanceMonitorAdvice() {
        PerformanceMonitorAdvice performanceMonitorAdvice = new PerformanceMonitorAdvice();
        performanceMonitorAdvice.setAppId("dispatcher.rmq.iapi.ymatou.com");
        performanceMonitorAdvice.setServerUrl(performanceServerUrl);
        performanceMonitorAdvice.setReportToCat(false);
        return performanceMonitorAdvice;
    }

    /*
    @Bean(name = "performancePointcut")
    public AspectJExpressionPointcut aspectJExpressionPointcut() {
        AspectJExpressionPointcut aspectJExpressionPointcut = new AspectJExpressionPointcut();

        aspectJExpressionPointcut.setExpression(
                "execution(* com.ymatou.mq.infrastructure.repository.*Repository.*(..))"
                        + "|| execution(* com.ymatou.mq.rabbit.receiver.service..*.*(..))"
        );

        return aspectJExpressionPointcut;
    }


    *//**
     * 对应xml
     * <aop:config>
     * <aop:advisor advice-ref="performanceMonitorAdvice"
     * pointcut-ref="performancePointcut" />
     * </aop:config>
     *
     * @return
     *//*
    @Bean
    public Advisor performanceMonitorAdvisor() {
        return new DefaultPointcutAdvisor(aspectJExpressionPointcut(), performanceMonitorAdvice());
    }
*/
}

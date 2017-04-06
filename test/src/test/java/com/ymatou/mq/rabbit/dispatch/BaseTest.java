package com.ymatou.mq.rabbit.dispatch;/*
 *
 *  (C) Copyright 2017 Ymatou (http://www.ymatou.com/).
 *  All rights reserved.
 *
 */

import org.junit.runner.RunWith;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

/**
 * @author luoshiqian 2017/3/27 16:34
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = BaseTest.class)
@Configuration
@ComponentScan(basePackages = "com.ymatou")
public class BaseTest {
}

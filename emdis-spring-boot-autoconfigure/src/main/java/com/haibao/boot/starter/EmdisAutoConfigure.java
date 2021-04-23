package com.haibao.boot.starter;

import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 *
 *
 * @author ml.c
 * @date 4:51 PM 4/23/21
 **/
@Configuration
//@ConditionalOnClass(EmdisService.class)
@EnableConfigurationProperties(EmdisProperties.class)
public class EmdisAutoConfigure {
}

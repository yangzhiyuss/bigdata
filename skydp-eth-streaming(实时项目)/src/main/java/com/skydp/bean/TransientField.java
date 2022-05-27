package com.skydp.bean;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.FIELD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 写入数据的时候，类中成员变量不需要sink到其他组件中，可以用该注解标记
 */
@Target(FIELD)
@Retention(RUNTIME)
public @interface TransientField {
}

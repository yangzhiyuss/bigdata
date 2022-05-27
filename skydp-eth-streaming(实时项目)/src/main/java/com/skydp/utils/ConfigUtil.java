package com.skydp.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Objects;
import java.util.Properties;


public class ConfigUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConfigUtil.class);
    private static final String CONFIG_FILE = "config.properties";

    /**
     * @return Properties
     * 获取配置文件
     */
    private static Properties loadProps() {
        Properties props = null;
        InputStreamReader in = null;
        try {
            in = new InputStreamReader(
                    Objects.requireNonNull(
                            Thread.currentThread()
                                    .getContextClassLoader().getResourceAsStream(CONFIG_FILE)));

            props = new Properties();
            props.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return props;
    }

    /**
     * @param key:关键字 defaultValue：默认值
     * @return Int
     * 获取配置文件中的值
     */
    public static int getIntConfig(String key, int defaultValue) {
        int result = defaultValue;
        try {
            Properties props = loadProps();
            String property = props.getProperty(key);
            result = new Integer(property);
        } catch (Exception e) {
            LOGGER.error("load properties file failure", e);
        }
        return result;
    }

    /**
     * @param key:关键字 defaultValue：默认值
     * @return Long
     * 获取配置文件中的值
     */
    public static long getLongConfig(String key, long defaultValue) {
        long result = defaultValue;
        try {
            Properties props = loadProps();
            String property = props.getProperty(key);
            result = new Long(property);
        } catch (Exception e) {
            LOGGER.error("load properties file failure", e);
        }
        return result;
    }

    /**
     * @param key:关键字
     * @return String 默认为null
     * 获取配置文件中的值
     */
    public static String getStringConfig(String key) {
        String result = "";
        try {
            Properties props = loadProps();
            result = props.getProperty(key);
        } catch (Exception e) {
            LOGGER.error("load properties file failure", e);
        }
        return result;
    }
}

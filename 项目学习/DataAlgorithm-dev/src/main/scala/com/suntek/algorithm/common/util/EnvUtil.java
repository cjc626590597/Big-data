package com.suntek.algorithm.common.util;

import org.apache.commons.lang3.StringUtils;

/**
 * Created with IntelliJ IDEA.
 * Author: yuanweilin
 * Date: 2020-4-8 10:35
 * Description:
 */
public class EnvUtil {

    public static String getEnv(String param){

        if(!StringUtils.isBlank(param)){
            String envValue = System.getenv(param);
            if(StringUtils.isBlank(envValue)){
                return System.getProperty(param);
            }else {
                return envValue;
            }
        }
        return null;
    }
}



package org.example.hadoop.mr.Ecommerce.utils;

import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 日志解析
 */
public class LogParser {
    public Map<String, String> parser(String log){
        IPParser ipParser = IPParser.getInstance();

        Map<String, String> logInfo = new HashMap<>();

        String[] splits = log.split("\001");

        if(StringUtils.isNotBlank(log)){
            // 解析ip
            String ip = splits[13];
            IPParser.RegionInfo regionInfo = ipParser.analyseIp(ip);

            String country = "";
            String province = "";
            String city = "";
            if(regionInfo != null){
                country = regionInfo.getCountry() != null ? regionInfo.getCountry() : "";
                province = regionInfo.getProvince() != null ? regionInfo.getProvince() : "";
                city = regionInfo.getCity() != null ? regionInfo.getCity() : "";
            }

            String url = splits[1];
            String pageId = ContentUtils.getPageId(url);
            String time = splits[17];
            logInfo.put("ip", ip);
            logInfo.put("store", StringUtils.isNotBlank(splits[3]) ? splits[3] : null);
            logInfo.put("browser", StringUtils.isNotBlank(splits[29]) ? splits[29] : null);
            logInfo.put("system", StringUtils.isNotBlank(splits[30]) ? splits[30] : null);
            logInfo.put("country", country);
            logInfo.put("province", province);
            logInfo.put("city", city);
            logInfo.put("url", url);
            logInfo.put("pageId", pageId);
            logInfo.put("time", time);
        }else{
            logInfo.put("ip", null);
            logInfo.put("store",  null);
            logInfo.put("browser",  null);
            logInfo.put("system", null);
            logInfo.put("country", null);
            logInfo.put("province", null);
            logInfo.put("city", null);
            logInfo.put("url", null);
            logInfo.put("pageId", null);
            logInfo.put("time", null);
        }

        return logInfo;
    }

    /**
     * 预处理过的数据
     * @param log 预处理过的log
     * @return
     */
    public Map<String, String> parserV2(String log){
        // 保存信息
        Map<String, String> logInfo = new HashMap<>();

        String[] splits = log.split("\t");

        if(splits.length >= 10){
            logInfo.put("ip", splits[0]);
            logInfo.put("store",  splits[1]);
            logInfo.put("browser",  splits[2]);
            logInfo.put("system", splits[3]);
            logInfo.put("country", splits[4]);
            logInfo.put("province", splits[5]);
            logInfo.put("city", splits[6]);
            logInfo.put("time", splits[7]);
            logInfo.put("url", splits[8]);
            logInfo.put("pageId", splits[9]);
        }else{
            logInfo.put("ip", null);
            logInfo.put("store",  null);
            logInfo.put("browser",  null);
            logInfo.put("system", null);
            logInfo.put("country", null);
            logInfo.put("province", null);
            logInfo.put("city", null);
            logInfo.put("time", null);
            logInfo.put("url", null);
            logInfo.put("pageId", null);
        }


        return logInfo;
    }
}

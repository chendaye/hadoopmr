package org.example.hadoop.hdfs.parms;

import java.io.IOException;
import java.util.Properties;

/**
 * 读取配置文件
 */
public class ParamsUtils {
    private static Properties properties = new Properties();

    /*static{}(即static块)，会在类被加载的时候执行且仅会被执行一次，一般用来初始化静态变量和调用静态方法*/
    static {
        try {
            properties.load(ParamsUtils.class.getClassLoader().getResourceAsStream("wc.properties"));
        }catch (IOException io){
            io.printStackTrace();
        }
    }

    /**
     * getter
     * @return 自定义配置
     * @throws Exception
     */
    public static Properties getProperties() throws Exception{
        return  properties;
    }
}

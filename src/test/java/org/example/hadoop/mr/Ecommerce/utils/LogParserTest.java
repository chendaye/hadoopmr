package org.example.hadoop.mr.Ecommerce.utils;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

public class LogParserTest {

    LogParser logParser = null;

    @Before
    public void setUp(){
        this.logParser = new LogParser();
    }

    @After
    public void tearDown(){
        this.logParser = null;
    }

    @Test
    public void testRegion(){
        String s = "20946835322^Ahttp://www.yihaodian.com/1/?tracker_u=2225501&type=3^Ahttp://www.baidu.com/s?wd=1%E5%8F%B7%E5%BA%97&rsv_bp=0&ch=&tn=baidu&bar=&rsv_spt=3&ie=utf-8&rsv_sug3=5&rsv_sug=0&rsv_sug1=4&rsv_sug4=313&inputT=4235^A1号店^A1^ASKAPHD3JZYH9EE9ACB1NGA9VDQHNJMX1NY9T^A^A^A^A^APPG4SWG71358HGRJGQHQQBXY9GF96CVU^A2225501^A\\N^A124.79.172.232^A^Amsessionid:YR9H5YU7RZ8Y94EBJNZ2P5W8DT37Q9JH,unionKey:2225501^A^A2013-07-21 09:30:01^A\\N^Ahttp://www.baidu.com/s?wd=1%E5%8F%B7%E5%BA%97&rsv_bp=0&ch=&tn=baidu&bar=&rsv_spt=3&ie=utf-8&rsv_sug3=5&rsv_sug=0&rsv_sug1=4&rsv_sug4=313&inputT=4235^A1^A^A\\N^Anull^A-10^A^A^A^A^AMozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; MATP; Media Center PC 6.0; .NET4.0C; InfoPath.2; .NET4.0E)^AWin32^A^A^A^A^A^A上>海市^A1^A^A2013-07-21 09:30:01^A上海市^A^A66^A^A^A^A^A\\N^A\\N^A\\N^A\\N^A2013-07-21";
        String[] info = s.split("\\^A");
        int index = 0;
        for (String val:info){
            System.out.println(index+"======="+val);
            index += 1;
        }

        Map<String, String> map = logParser.parser(s);
        for (Map.Entry<String, String> entry: map.entrySet()){
//            System.out.println(entry.getKey()+":"+entry.getValue());
        }
    }

    @Test
    public void testV2(){
        String str = "123.118.107.1\tnull\tMozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0; .NET4.0E; .NET4.0C; .NET CLR 3.5.30729; .NET CLR 2.0.50727; .NET CLR 3.0.30729; InfoPath.3; ASU2JS)\tWin32\t中国\t北京市\t\t2013-07-21 18:54:48\thttp://www.yihaodian.com/cart/cart.do?action=view\t\t\n";
        String[] info = str.split("\t");

        long index = 0;
        for (String val : info){
            System.out.println(index+">>>>>>>>>>>"+val);
            index += 1;
        }
    }

    @Test
    public void testV3(){
        String str = "123.118.107.1\tnull\tMozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.2; WOW64; Trident/6.0; .NET4.0E; .NET4.0C; .NET CLR 3.5.30729; .NET CLR 2.0.50727; .NET CLR 3.0.30729; InfoPath.3; ASU2JS)\tWin32\t中国\t北京市\t\t2013-07-21 18:54:48\thttp://www.yihaodian.com/cart/cart.do?action=view\t\t\n";
//        String[] info = str.split("\t");

        Map<String, String> map = logParser.parserV2(str);
        for (Map.Entry<String, String> entry: map.entrySet()){
            System.out.println(entry.getKey()+":"+entry.getValue());
        }
    }
}

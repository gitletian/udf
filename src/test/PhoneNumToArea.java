package test;


import org.apache.hadoop.hive.ql.exec.UDF;  
//club.drguo.hive.PhoneNumToArea  
public class PhoneNumToArea extends UDF{  
//    private static HashMap<String, String> areaMap = new HashMap<>();  
//    static{  
//        areaMap.put("136", "北京");  
//        areaMap.put("137", "南京");  
//        areaMap.put("138", "东京");  
//    }  
    //方法要用public修饰！！！  
    public String evaluate(String phoneNum) {  
        String result = "test";  
        return result;  
    }  
}  
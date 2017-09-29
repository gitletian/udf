package com.marcpoint.elengjing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;
import org.apache.hadoop.io.Text;

  
public class SkuMonthChange1 extends UDAF {
	
	public static class UDAFSkuData {
		private ArrayList<HashMap<String, String>> skuDate = new ArrayList<HashMap<String, String>>();
    }
	
    public static class SkuMonthChangeUDAFEvaluator implements UDAFEvaluator {  
  
    	UDAFSkuData udafskudata;  
        
        public SkuMonthChangeUDAFEvaluator() {  
            super();  
            udafskudata = new UDAFSkuData();  
            init();  
        }  
  
        /** 
         * Reset the state of the aggregation. 
         */  
        public void init() {  
        	udafskudata.skuDate = new ArrayList<HashMap<String, String>>();
        }
        
  
        /** 
         * Iterate through one row of original data. 
         *  
         * The number and type of arguments need to the same as we call this 
         * UDAF from Hive command line. 
         *  
         * This function should always return true. 
         */  
        public boolean iterate(String skulist) {  
            if (skulist != null && !"".equals(skulist.trim())) {
            	HashMap<String, String> skuMap = new HashMap<String, String>();
            	String[] sku_array = skulist.split("&&");
            	for(String skustr : sku_array){
            		String[] sku = skustr.split("=");
            		if(sku.length == 3){
            			skuMap.put(sku[0], sku[2]);
            		}
            	}
            	
            	udafskudata.skuDate.add(skuMap);
            }  
            return true;  
        }
        
  
        /** 
         * Terminate a partial aggregation and return the state. If the state is 
         * a primitive, just return primitive Java classes like Integer or 
         * String. 
         */  
        public UDAFSkuData terminatePartial() {  
            // This is SQL standard - average of zero items should be null.  
            return udafskudata.skuDate == null ? null : udafskudata;  
        }  
  
        /** 
         * Merge with a partial aggregation. 
         *  
         * This function should always have a single argument which has the same 
         * type as the return value of terminatePartial(). 
         *  
         * 合并点评平均价格列表 
         */  
        public boolean merge(UDAFSkuData o) {  
            if (o != null) {  
            	udafskudata.skuDate.addAll(o.skuDate);  
            }  
            return true;  
        }  
  
        public Text terminate() {
        	try {
        		HashMap<String, String> skuEndMap = new HashMap<>();
            	for(HashMap<String, String> skuMap:udafskudata.skuDate){
            		Iterator<Entry<String, String>> entries = skuMap.entrySet().iterator();
            		while(entries.hasNext()){
            			Entry<String, String> entry = entries.next();
            			String key = entry.getKey();
            			String value = entry.getValue();
            			
            			if(skuEndMap.containsKey(key)){
            				Integer v = Integer.valueOf(skuEndMap.get(key)) + Integer.valueOf(value);
            				skuEndMap.put(key, String.valueOf(v));
            			}else{
            				skuEndMap.put(key, value);
            			}
            		}
            	}
            	
            	Iterator<Entry<String, String>> entries = skuEndMap.entrySet().iterator();
            	
            	StringBuffer endStr = new StringBuffer();
            	while(entries.hasNext()){
            		Entry<String, String> entry = entries.next();
            		endStr.append("&&" + entry.getKey() + "=" + entry.getValue());
            	}
            	String  ss = endStr.toString();
            	if ("" != ss && ss.length() > 2){
            		ss = ss.substring(2);
            	}else{
            		ss = "";
            	}
            	return new Text(ss);
			} catch (Exception e) {
				// TODO: handle exception
				return new Text(e.toString());
			}
        	
        }  
    }
  
}  

package com.marcpoint.elengjing_new_2;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;

public class TaggedTool {
	Pattern pattern = Pattern.compile("^([a-zA-Z0-9]+)|([a-zA-Z0-9]+)$", Pattern.CASE_INSENSITIVE);
	public static Properties getUdfConfig(String configPath){
		Properties udfConfig = new Properties();
		InputStream is = null;
		try {
			is = new FileInputStream(configPath);
			udfConfig.load(is);
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}finally{
			if (is != null){
				try {
					is.close();
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		return udfConfig;
	}
	
	public static String getAttrDictString(String filePath){
		String attrDictStr = null;
		File file = new File(filePath);
        Scanner scanner = null;
        StringBuilder buffer = new StringBuilder();
        try {
            scanner = new Scanner(file, "utf-8");
            while (scanner.hasNextLine()) {
                buffer.append(scanner.nextLine());
            }
            attrDictStr = buffer.toString();
            
        } catch (FileNotFoundException e) {
            // TODO Auto-generated catch block  
//        	System.out.println("error"+e.getMessage());
        	e.printStackTrace();
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
        return attrDictStr;
	}
	
	
	public static Map<Long,HashMap<Long, HashMap<String, HashMap<String, String>>>> paraAttrDict(String attrDictStr){
		
		Map<Long,HashMap<Long, HashMap<String, HashMap<String, String>>>> attrAllDic = new HashMap<Long,HashMap<Long, HashMap<String, HashMap<String, String>>>>();
		if (null != attrDictStr){
			try {
				JSONObject industryIDjo = new JSONObject(attrDictStr);
				Iterator<String> iit = industryIDjo.keys();
				while(iit.hasNext()){
					String key = iit.next();
					JSONObject cjo = industryIDjo.getJSONObject(key);
					Iterator<String> cit = cjo.keys();
					HashMap<Long, HashMap<String, HashMap<String, String>>> categoryMap = new HashMap<Long,HashMap<String, HashMap<String, String>>>();
					while(cit.hasNext()){
						String ckey = cit.next();
						JSONObject attrNameObj = cjo.getJSONObject(ckey);
						Iterator<String> attrNameObjit = attrNameObj.keys();
						HashMap<String, HashMap<String, String>> attrNameMap = new HashMap<String, HashMap<String, String>>();
						while(attrNameObjit.hasNext()){
							String attrName = attrNameObjit.next();
							JSONObject attrValueObj = attrNameObj.getJSONObject(attrName);
							Iterator<String> attrValueObjit = attrValueObj.keys();
							HashMap<String, String> attrValueMap = new HashMap<String, String>();
							while(attrValueObjit.hasNext()){
								String av = attrValueObjit.next();
								String desc = attrValueObj.getString(av);
								attrValueMap.put(ToDBC(av), desc);
							}
							attrNameMap.put(attrName, attrValueMap);
						}
						categoryMap.put(Long.parseLong(ckey), attrNameMap);
					}
					attrAllDic.put(Long.parseLong(key), categoryMap);
				}
				
			} catch (JSONException je) {
				// TODO: handle exception
				je.printStackTrace();
			}
		}
		return attrAllDic;
	}
	

	public HashMap<String, String> parasValue(HashMap<String, String> attrValueMap, String attrName, String attrValue){
		HashMap<String, String> attrValues = new HashMap<String, String> ();
		
		if (attrValueMap.containsKey(attrValue)) {
			attrValues.put(attrValue, attrValueMap.get(attrValue));
			return attrValues;
		}
		
//		List<Object> attrList = Arrays.asList(attrValueMap.keySet().toArray());
//		Collections.sort(attrList, new Comparator<Object>() {
//	        @Override
//	        public int compare(Object o1, Object o2) {
//	          return o1.toString().length() - o2.toString().length();
//	        }
//	      });
		
		// 特殊处理 "尺码", "尺寸"
//		if(Arrays.asList(new String[]{"尺码", "尺寸"}).contains(attrName)){
//			for(String attrv : attrValue.split(" ")){
//				attrv = ToDBC(attrv);
//				Matcher matcher = pattern.matcher(attrv);
//				while(matcher.find()){
//					String matcher_str = matcher.group();
//					for(Object attr: attrList){
//						String attrStr = attr.toString();
//						if (!"".equals(attrv) && attrStr.indexOf(matcher_str) > -1){
//							attrValues.put(attrStr, attrValueMap.get(attrStr));
//							break;
//						}
//					}
//				}
//			}
//		}else{
//			for(String attrv : attrValue.split(" ")){
//				for(Object attr: attrList){
//					String attrStr = attr.toString();
//					if (!"".equals(attrv) && attrStr.indexOf(attrv) > -1){
//						attrValues.put(attrStr, attrValueMap.get(attrStr));
//						break;
//					}
//				}
//			}
//		}
		
		for(String attrv : attrValue.split(" | |，| , |, | ,|,")){
			if (attrValueMap.containsKey(attrv)){
				attrValues.put(attrv, attrValueMap.get(attrv));
			}
		}
		
		return attrValues;
		
	}
	
	 /**
     * 全角转半角
     * @param input String.
     * @return 半角字符串
     */
    public static String ToDBC(String input) {
             char c[] = input.toCharArray();
             for (int i = 0; i < c.length; i++) {
               if (c[i] == '\u3000') {
                 c[i] = ' ';
               } else if (c[i] > '\uFF00' && c[i] < '\uFF5F') {
                 c[i] = (char) (c[i] - 65248);

               }
             }
        String returnString = new String(c);
        return returnString;
    }
	
	public HashMap<String, HashMap<String, String>> parasDesc(Long itemId, String itemAttrDesc, HashMap<String, HashMap<String, String>> attrDic, String[] separator){
		HashMap<String, HashMap<String, String>> parasItem = new HashMap<String, HashMap<String, String>>();
		String[] attrLists = itemAttrDesc.split(separator[0]);
		for(String attr:attrLists){
			String[] attrList = attr.split(separator[1]);
			if (attrList.length > 1){
				String attrName = attrList[0];
				String attrValue = attrList[1];
				
				if (attrValue.length()<=512){
					if(attrDic.containsKey(attrName)){
						HashMap<String, String> attrValueMap = attrDic.get(attrName);
						HashMap<String, String> attrValues = parasValue(attrValueMap, attrName, attrValue);
						if(!attrValues.isEmpty()){
							if(parasItem.keySet().contains(attrName)){
								parasItem.get(attrName).putAll(attrValues);;
							}else{
								parasItem.put(attrName, attrValues);
							}
						}
					}
				}
			}
		}
		return parasItem;
	}
	
	
	public HashMap<String, HashMap<String, String>> parasItemName(Long itemId, String itemName, HashMap<String, HashMap<String, String>> attrDic){
		HashMap<String, HashMap<String, String>> parasItem = new HashMap<String, HashMap<String, String>>();
		Iterator<Entry<String, HashMap<String, String>>> entries = attrDic.entrySet().iterator();
		while(entries.hasNext()){
			Entry<String, HashMap<String, String>> entrie = entries.next();
			HashMap<String, String> attrValeMap = entrie.getValue();
			for(String attr:attrValeMap.keySet()){
				if(itemName.indexOf(attr) > -1){
					HashMap<String, String> data = new HashMap<String, String>();
					data.put(attr, attrValeMap.get(attr));
					String key = entrie.getKey();
					if(parasItem.containsKey(key)){
						parasItem.get(key).putAll(data);;
					}else{
						parasItem.put(key, data);
					}
					itemName = itemName.replace(attr, "");
				}
			}
		}
		return parasItem;
	}

}

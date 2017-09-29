package com.marcpoint.elengjing_extend;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.json.JSONException;
import org.json.JSONObject;

public class TaggedTool_e {
	private ArrayList<String> fengge = new ArrayList<String>(Arrays.asList(new String[]{"通勤", "甜美", "街头"}));
	
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
	
    

	public HashMap<String, ArrayList<String>> parasValue(HashMap<String, ArrayList<String>> parasItem, HashMap<String, String> attrValueMap, String attrName, String attrValue){
		
		if (attrValueMap.containsKey(attrValue)) {
			String attr_desc = attrValueMap.get(attrValue);
			if(parasItem.containsKey(attr_desc)){
				if(!parasItem.get(attr_desc).contains(attrValue)){
					parasItem.get(attr_desc).add(attrValue);
				}
			}else{
				ArrayList<String> al = new ArrayList<>();
				al.add(attrValue);
				parasItem.put(attr_desc, al);
				
			}
			return parasItem;
		}
		
		for(String attrv : attrValue.split(" | |，| , |, | ,|,")){
			if (attrValueMap.containsKey(attrv)){
				String attr_desc = attrValueMap.get(attrv);
				if(parasItem.containsKey(attr_desc)){
					if(!parasItem.get(attr_desc).contains(attrv)){
						parasItem.get(attr_desc).add(attrv);
					}
				}else{
					ArrayList<String> al = new ArrayList<>();
					al.add(attrv);
					parasItem.put(attr_desc, al);
				}
			}
		}
		return parasItem;
		
	}

	public HashMap<String, String> parasDesc(Long itemId, String itemAttrDesc, HashMap<String, HashMap<String, String>> attrDic, String[] separator){
		
		HashMap<String, ArrayList<String>> parasItem = new HashMap<String, ArrayList<String>>();
		String[] attrLists = itemAttrDesc.split(separator[0]);
		for(String attr:attrLists){
			String[] attrList = attr.split(separator[1]);
			if (attrList.length > 1){
				String attrName = attrList[0];
				String attrValue = attrList[1];
				
				if (attrValue.length()<=512){
					if(attrDic.containsKey(attrName)){
						HashMap<String, String> attrValueMap = attrDic.get(attrName);
						parasValue(parasItem, attrValueMap, attrName, attrValue);
					}
				}
			}
		}
		
		return HSparasItem(parasItem);
	}
	
	public HashMap<String, String> parasItemName(Long itemId, String itemName, HashMap<String, HashMap<String, String>> attrDic){
		HashMap<String, ArrayList<String>> parasItem = new HashMap<String, ArrayList<String>>();
		Iterator<Entry<String, HashMap<String, String>>> entries = attrDic.entrySet().iterator();
		while(entries.hasNext()){
			Entry<String, HashMap<String, String>> entrie = entries.next();
			HashMap<String, String> attrValeMap = entrie.getValue();
			for(String attr:attrValeMap.keySet()){
				if(itemName.indexOf(attr) > -1){
					String attr_desc = attrValeMap.get(attr);
					if(parasItem.containsKey(attr_desc)){
						if(!parasItem.get(attr_desc).contains(attr)){
							parasItem.get(attr_desc).add(attr);
						}
					}else{
						ArrayList<String> al = new ArrayList<>();
						al.add(attr);
						parasItem.put(attr_desc, al);
					}
					
					itemName = itemName.replace(attr, "");
				}
			}
		}
		return HSparasItem(parasItem);
	}

	
	private HashMap<String, String> HSparasItem(HashMap<String, ArrayList<String>> parasItem){
		HashMap<String, String> newParsItem = new HashMap<String, String>();
		Iterator<Entry<String, ArrayList<String>>> entrys = parasItem.entrySet().iterator();
		while(entrys.hasNext()){
			Entry<String, ArrayList<String>> entry = entrys.next();
			String attrname = entry.getKey();
			ArrayList<String> attrValue_list = new ArrayList<String>(entry.getValue());
			
			if("风格".equals(attrname)){
				ArrayList<String> newList = (ArrayList<String>)attrValue_list.clone();
				newList.removeAll(fengge);
				if(!newList.isEmpty()){
					attrValue_list.removeAll(fengge);
				}
			}
//			Collections.sort(attrValue_list);
//			String attrValue = StringUtils.join(attrValue_list, ",");
//			newParsItem.put(attrname, attrValue);
			if(!attrValue_list.isEmpty()){
				newParsItem.put(attrname, attrValue_list.get(0));
			}
		}
		return newParsItem;
	}
	
}

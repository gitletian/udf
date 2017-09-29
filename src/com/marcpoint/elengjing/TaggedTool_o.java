package com.marcpoint.elengjing;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Scanner;
import java.util.Map.Entry;
import java.util.regex.Pattern;

import org.json.JSONException;
import org.json.JSONObject;

public class TaggedTool_o {
	Pattern pattern = Pattern.compile("^([a-zA-Z0-9]+)|([a-zA-Z0-9]+)$", Pattern.CASE_INSENSITIVE);
	
	static ArrayList<String> attr_list = new ArrayList<String>(){{add("通勤");add("街头");add("甜美");}};
	
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
	
	
	public static Map<Long,HashMap<Long, HashMap<String, List<String>>>> paraAttrDict(String attrDictStr){
		Map<Long,HashMap<Long, HashMap<String, List<String>>>> attrAllDic = new HashMap<Long,HashMap<Long, HashMap<String, List<String>>>>();
		if (null != attrDictStr){
			try {
				JSONObject industryIDjo = new JSONObject(attrDictStr);
				Iterator<String> iit = industryIDjo.keys();
				while(iit.hasNext()){
					String key = iit.next();
					JSONObject cjo = industryIDjo.getJSONObject(key);
					Iterator<String> cit = cjo.keys();
					HashMap<Long, HashMap<String, List<String>>> categoryMap = new HashMap<Long,HashMap<String, List<String>>>();
					while(cit.hasNext()){
						String ckey = cit.next();
						JSONObject djo = cjo.getJSONObject(ckey);
						Iterator<String> dit = djo.keys();
						HashMap<String, List<String>> attrMap = new HashMap<String,List<String>>();
						while(dit.hasNext()){
							String dkey = dit.next();
							String dvalue = djo.getString(dkey);
							
							attrMap.put(dkey, Arrays.asList(dvalue.split(",")));
						}
						categoryMap.put(Long.parseLong(ckey), attrMap);
					}
					attrAllDic.put(Long.parseLong(key), categoryMap);
				}
				
			} catch (JSONException je) {
				// TODO: handle exception
//				System.out.println("parsing error. error message is ="+je.getMessage());
				je.printStackTrace();
			}
		}
		return attrAllDic;
	}
	
	
	public ArrayList<String> parasValue(List<String> attrAllvalueList, String attrName, String attrValue){
		ArrayList<String> attrValues = new ArrayList<String>();
		
		for(String attrv : attrValue.split(" | |，| , |, | ,|,")){
			if(attrAllvalueList.contains(attrv)){
				attrValues.add(attrv);
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
	
	public HashMap<String, ArrayList<String>> parasDesc(Long itemId, String itemAttrDesc, HashMap<String, List<String>> attrDic, String[] separator){
		HashMap<String, ArrayList<String>> parasItem = new HashMap<String, ArrayList<String>>();
		String[] attrLists = itemAttrDesc.split(separator[0]);
		for(String attr:attrLists){
			String[] attrList = attr.split(separator[1]);
			if (attrList.length > 1){
				String attrName = attrList[0];
				String attrValue = attrList[1];
				
				if (attrValue.length()<=512){
					if(attrDic.containsKey(attrName) || this.attr_list.contains(attrName)){
						List<String> attrAllvalueList = attrDic.get(attrName);
						
						if(this.attr_list.contains(attrName)){
							if(attrDic.containsKey("风格")){
								attrAllvalueList = attrDic.get("风格");
							}else if(attrDic.containsKey("中老年风格")){
								attrAllvalueList = attrDic.get("中老年风格");
							}
						}
						
						ArrayList<String> attrValues = parasValue(attrAllvalueList, attrName, attrValue);
						
						if(!attrValues.isEmpty()){
							if(parasItem.keySet().contains(attrName)){
								parasItem.get(attrName).addAll(attrValues);
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
	
	
	public HashMap<String, ArrayList<String>> parasItemName(Long itemId, String itemName, HashMap<String, List<String>> attrDic){
		HashMap<String, ArrayList<String>> parasItem = new HashMap<String, ArrayList<String>>();
		Iterator<Entry<String, List<String>>> entries = attrDic.entrySet().iterator();
		while(entries.hasNext()){
			Entry<String, List<String>> entrie = entries.next();
			List<String> attrList = entrie.getValue();
			for(String attr:attrList){
				if(!attr.equals("") && itemName.indexOf(attr) > -1){
					String key = entrie.getKey();
					if(parasItem.keySet().contains(key)){
						parasItem.get(key).add(attr);
					}else{
						ArrayList<String> attrValues = new ArrayList<String>();
						attrValues.add(attr);
						parasItem.put(key, attrValues);
					}
					itemName = itemName.replace(attr, "");
				}
			}
		}
		return parasItem;
		
	}
	

	public HashMap<String, ArrayList<String>> special_handling(HashMap<String, ArrayList<String>> parasItem){
		
		for(String attr: this.attr_list){
			if(!parasItem.containsKey(attr)){
				continue;
			}
			
			String[] fengge = {"风格", "中老年风格"};
			for(String fg: fengge){
				if(parasItem.containsKey(fg)){
					parasItem.get(fg).addAll(parasItem.get(attr));
					parasItem.get(fg).remove(attr);
					parasItem.remove(attr);
					break;
				}
			}
		}
		return parasItem;
	}
	
	


}

package com.marcpoint.elengjing;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.mapred.JobConf;

public class Tagged_o extends GenericUDTF {
	/**
	 * 
	 * set elengjing.var.separator_conf=new;
	 * set elengjing.var.separator_conf=old;
	 * 
	 * */
	public static Map<Long,HashMap<Long, HashMap<String, List<String>>>> attrAllDic = null;
	TaggedTool_o taggedTool = new TaggedTool_o();
	String[] separator = null;
	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
	}
	
	@Override
	public void configure(MapredContext context) {
		// TODO Auto-generated method stub
		if(null != context){
			this.separator = new String[] {" && ", ": "};
			JobConf conf = new JobConf(context.getJobConf());
			String separator_conf = conf.get("elengjing.var.separator_conf");
			if(null != separator_conf && separator_conf.equalsIgnoreCase("old")){
				this.separator = new String[] {";", ":"};
			}
		}else{
			System.out.println("=========configure========context is null =============");
		}
	}
	
	@Override
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		// TODO Auto-generated method stub
	     if (args.length != 5) {
	         throw new UDFArgumentLengthException("ExplodeMap takes only 6 argument");
	     }
	     for(ObjectInspector arg:args){
	    	 if (arg.getCategory() != ObjectInspector.Category.PRIMITIVE) {
				 throw new UDFArgumentException("ExplodeMap takes string as a parameter");
			 }
	     }
	     
	     try {
	    	 FileSystem fs = FileSystem.get(new Configuration());
	    	 FSDataInputStream in = fs.open(new Path("/data/industryattr_old_old.json"));
			 BufferedReader configFile = new BufferedReader(new InputStreamReader(in));
			 StringBuilder buffer = new StringBuilder();
			 int c = 0;
			 while((c = configFile.read())!=-1){
				 buffer.append((char)c);
			 }
			 configFile.close();
			 attrAllDic = TaggedTool_o.paraAttrDict(buffer.toString());
			 
		} catch (Exception e) {
			// TODO: handle exception
			System.out.println("=====================print error");
		}

	     
		 
		 ArrayList<String> fieldNames = new ArrayList<String>();
		 ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		 fieldNames.add("itemId");
		 fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		 fieldNames.add("shopID");
		 fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		 fieldNames.add("categoryID");
		 fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		 fieldNames.add("attrName");
		 fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		 fieldNames.add("attrValue");
		 fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		 fieldNames.add("execeptInfo");
		 fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		 return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
		
	}

	@Override
	public void process(Object[] args) throws HiveException {
		// TODO Auto-generated method stub 
		Long itemId = null;
		String message = "";
		try {
			if(!attrAllDic.isEmpty()){
				if (args.length == 5){
					if (null != args[0] && null != args[1]){
						itemId = Long.parseLong(args[0].toString());
						Long categoryID = Long.parseLong(args[1].toString());
						Long shopID = Long.parseLong(args[2].toString());
						
						String itemName = null == args[3] ? "" : args[3].toString();
						String itemAttrDesc = null == args[4] ? "" : args[4].toString();
						HashMap<Long,HashMap<String, List<String>>> induMap = attrAllDic.get(16L);
						
						if (induMap.containsKey(categoryID)){
							HashMap<String, ArrayList<String>> parasItem = null;
							HashMap<String, List<String>> attrDic = induMap.get(categoryID);
							try {
								
								if(!"".equals(itemAttrDesc) || !"".equals(itemName)){
									if (!"".equals(itemAttrDesc)){//对itemAttrDesc进行打标签
										parasItem = taggedTool.parasDesc(itemId, itemAttrDesc, attrDic, this.separator);
										
									}else if(!"".equals(itemName)) {//对itemName 进行打标签
										parasItem = taggedTool.parasItemName(itemId, itemName, attrDic);
										
									}
									
									parasItem = taggedTool.special_handling(parasItem);
									
									if( null != parasItem && !parasItem.isEmpty()){
										Iterator<Entry<String, ArrayList<String>>> entrys = parasItem.entrySet().iterator();
										while(entrys.hasNext()){
											Entry<String, ArrayList<String>> entry = entrys.next();
											for(String attrValue:entry.getValue()){
												forward(new String[]{itemId.toString(), shopID.toString(), categoryID.toString(), entry.getKey(), attrValue, null});
											}
										}
									}else{
										message = "paras is null =itemAttrDesc="+itemAttrDesc+"==itemName="+itemName;
									}
									
								}else{
									message = "itemAttrDesc is null and itemName is null";
								}
							} catch (Exception e) {
								String ss = itemId == null ? "":itemId.toString();
								StringWriter sw = new StringWriter();
								PrintWriter pw = new PrintWriter(sw);
								e.printStackTrace(pw);
								forward(new String[]{ss, null, null, null, null, "unknow error=="+sw.toString()});
							}
							
						}else{
							message = "categoryID error ,categoryID not in attrDic==";
						}
					}else{
						message = "itemId or categoryID is null ==itemId=="+args[0]+"=categoryID="+args[1];
					}
					
				}else{
					message = "param is error,must have 4";
				}
			}else{
				message = "attrAllDic file is null";
			}
			
			if("" != message){
				if(null == itemId){
					forward(new String[]{null, null, null, null, null, message});
				}else{
					forward(new String[]{itemId.toString(), null, null, null, null, message});
				}
			}
			
			
		} catch (Exception e) {
			// TODO: handle exception
			String ss = itemId == null ? "":itemId.toString();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			forward(new String[]{ss, null, null, null, null, "unknow error=="+sw.toString()});
		}
		
		
	}
	
	
}

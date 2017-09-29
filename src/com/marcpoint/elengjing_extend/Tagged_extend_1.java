package com.marcpoint.elengjing_extend;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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

public class Tagged_extend_1 extends GenericUDTF {
	/**
	 * 
	 * set elengjing.var.separator_conf=new;
	 * set elengjing.var.separator_conf=old;
	 * 
	 * */
	
	public static Map<Long,HashMap<Long, HashMap<String, HashMap<String, String>>>> attrAllDic = null;
	TaggedTool_e taggedTool = new TaggedTool_e();
	
	String[] separator = null;
	
	String[] outPutSeg = new String[] {"充绒量", "含绒量", "图案", "图案文化", "填充物", "工艺", "廓形", "成分含量", "摆型", "材质成分",
			"款式", "版型", "礼服摆型", "腰型", "衣门襟", "袖型", "袖长", "裙型", "裙长", "裤型",
			"裤长", "襟形", "里料", "面料", "领子", "风格", "适用年龄"};
	String[] output = new String[outPutSeg.length + 2];
	
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
	    	 FSDataInputStream in = fs.open(new Path("/data/industryattr_new_new.json"));
			 BufferedReader configFile = new BufferedReader(new InputStreamReader(in));
			 StringBuilder buffer = new StringBuilder();
			 int c = 0;
			 while((c = configFile.read())!=-1){
				 buffer.append((char)c);
			 }
			 configFile.close();
			 attrAllDic = TaggedTool_e.paraAttrDict(buffer.toString());
			 
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
		 
		 for(int i=0; i<outPutSeg.length; i++ ){
			 fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
			 fieldNames.add("attr" + i+1);
		 }
		 
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
						HashMap<Long,HashMap<String, HashMap<String, String>>> induMap = attrAllDic.get(16L);
						
						if (induMap.containsKey(categoryID)){
							HashMap<String, String> parasItem = null;
							HashMap<String, HashMap<String, String>> attrDic = induMap.get(categoryID);
							try {
								
								if(!"".equals(itemAttrDesc) || !"".equals(itemName)){
									if (!"".equals(itemAttrDesc)){//对itemAttrDesc进行打标签
										parasItem = taggedTool.parasDesc(itemId, itemAttrDesc, attrDic, this.separator);
									}else if(!"".equals(itemName)) {//对itemName 进行打标签
//										parasItem = taggedTool.parasItemName(itemId, itemName, attrDic);
									}
									
									if( null != parasItem && !parasItem.isEmpty()){
										ArrayList<String> output = new ArrayList<String>();
										output.add(itemId.toString());
										output.add(shopID.toString());
										output.add(categoryID.toString());
										for(String i: outPutSeg){
											output.add(parasItem.get(i));
										}
										output.add(null);
										forward(output);
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
								ArrayList<String> op = new ArrayList<>(Arrays.asList(this.output));
								op.add(0, ss);
								op.add("unknow error=="+sw.toString());
								forward(op);
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
				ArrayList<String> op = new ArrayList<>(Arrays.asList(this.output));
				if(null == itemId){
					op.add(0, null);
				}else{
					op.add(0, itemId.toString());
				}
				op.add(message);
				forward(op);
			}
			
			
		} catch (Exception e) {
			// TODO: handle exception
			String ss = itemId == null ? "":itemId.toString();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			ArrayList<String> op = new ArrayList<>(Arrays.asList(this.output));
			op.add(0, ss);
			op.add("unknow error=="+sw.toString());
			forward(op);
		}
	}
}

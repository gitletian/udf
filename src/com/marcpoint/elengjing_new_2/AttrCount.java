package com.marcpoint.elengjing_new_2;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hive.ql.Context;
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

public class AttrCount extends GenericUDTF {
	/**
	 * 
	 * set elengjing.var.separator_conf=new;
	 * set elengjing.var.separator_conf=old;
	 * 
	 * */
	
	public static Map<Long,HashMap<Long, HashMap<String, HashMap<String, String>>>> attrAllDic = null;
	TaggedTool taggedTool = new TaggedTool();
	String[] separator = null;
	
	@Override
	public void configure(MapredContext context) {
		// TODO Auto-generated method stub
		if(null != context){
			this.separator = new String[] {" && ", ": ", " | |，| , |, | ,|,"};
			JobConf conf = new JobConf(context.getJobConf());
			String separator_conf = conf.get("elengjing.var.separator_conf");
			if(null != separator_conf && separator_conf.equalsIgnoreCase("old")){
				this.separator = new String[] {";", ":", " | |，| , |, | ,|,"};
			}
		}else{
			System.out.println("=========configure========context is null =============");
		}
	}
	
	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
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
	    	 FSDataInputStream in = fs.open(new Path("/data/industryattr.json"));
			 BufferedReader configFile = new BufferedReader(new InputStreamReader(in));
			 StringBuilder buffer = new StringBuilder();
			 int c = 0;
			 while((c = configFile.read())!=-1){
				 buffer.append((char)c);
			 }
			 configFile.close();
			 attrAllDic = TaggedTool.paraAttrDict(buffer.toString());
			 
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
			if (args.length == 5){
				if (null != args[0] && null != args[1]){
					itemId = Long.parseLong(args[0].toString());
					Long categoryID = Long.parseLong(args[1].toString());
					Long shopID = Long.parseLong(args[2].toString());
					
					String itemName = null == args[3] ? "" : args[3].toString();
					String itemAttrDesc = null == args[4] ? "" : args[4].toString();
					
					String[] attrLists = itemAttrDesc.split(this.separator[0]);
					for(String attr:attrLists){
						String[] attrList = attr.split(this.separator[1]);
						if (attrList.length > 1){
							String attrName = attrList[0];
							String attrValue = attrList[1];
							
							if (attrValue.length()<=512){
								for(String attrv : attrValue.split(separator[2])){
									forward(new String[]{itemId.toString(), shopID.toString(), categoryID.toString(), attrName, attrv, null});
								}
							}
						}
					}
				}else{
					message = "itemId or categoryID is null ==itemId=="+args[0]+"=categoryID="+args[1];
				}
				
			}else{
				message = "param is error,must have 4";
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

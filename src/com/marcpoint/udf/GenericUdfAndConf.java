package com.marcpoint.udf;


import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;

import com.google.common.base.Objects;

import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

/**
 * 1、测试 接收 list 参数类型 数据
 * 2、测试 读取 beeline 中设置 的set  ，从 context 中读取
 */

@Description(name = "contain", value = "_FUNC_(List<T>, T) ")

public class GenericUdfAndConf extends GenericUDF {

	private static final long serialVersionUID = 1L;
	String shopid = null;
	private ListObjectInspector listObjectInspector;
	private StringObjectInspector stringObjectInspector;
	
	@Override
	public void configure(MapredContext context) {
		System.out.println("Inside configure()");
		if (null != context){
			System.out.println("Is it map?:" + context.isMap());
			JobConf conf = new JobConf(context.getJobConf());
			Iterator<Entry<String, String>> iter = conf.iterator();
			while (iter.hasNext()) {
				Entry<String, String> val = iter.next();
				System.out.println("=================JobConf: " + val.getKey() + " = " + val.getValue());
			}
			
			this.shopid = conf.get("hive.var.shopid");
			
		}else{
			System.out.println("=================context is null =============:");
		}
	}
	
	@Override
	public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
		// 参数个数校验
		if (arguments.length != 2) {
			throw new UDFArgumentLengthException(
					"The function 'Contain' only accepts 2 argument : List<T> and T , but got " + arguments.length);
		}

		ObjectInspector argumentOne = arguments[0];
		ObjectInspector argumentTwo = arguments[1];

		// 参数类型校验
		if (!(argumentOne instanceof ListObjectInspector)) {
			throw new UDFArgumentException("The first argument of function must be a list / array");
		}
		if (!(argumentTwo instanceof StringObjectInspector)) {
			throw new UDFArgumentException("The second argument of function must be a string");
		}

		this.listObjectInspector = (ListObjectInspector) argumentOne;
		this.stringObjectInspector = (StringObjectInspector) argumentTwo;
		
		// 链表元素类型检查
		if (!(listObjectInspector.getListElementObjectInspector() instanceof StringObjectInspector)) {
			throw new UDFArgumentException("The first argument must be a list of strings");
		}
		// 返回值类型
//		return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
		return PrimitiveObjectInspectorFactory.javaStringObjectInspector;
	}

	@Override
	public Object evaluate(DeferredObject[] arguments) throws HiveException {
		// 利用ObjectInspector从DeferredObject[]中获取元素值
		List<Text> list = (List<Text>) this.listObjectInspector.getList(arguments[0].get());
		String str = this.stringObjectInspector.getPrimitiveJavaObject(arguments[1].get());
		
		if (Objects.equal(list, null) || Objects.equal(str, null)) {
			return null;
		}
		
		// 判断是否包含查询元素
		/*
		for (Text lazyString : list) {
			String s = lazyString.toString();
			if (Objects.equal(str, s)) {
				return new Boolean(true);
			}
		}
		*/
		return this.shopid;
	}

	@Override
	public String getDisplayString(String[] children) {
		return "arrayContainsExample() strContain(List<T>, T)";
	}

}

/*
 * 使用：
 *    set hive.var.shopid=2352;
 *	  ConfVar_4(array("da", "bf", "ce"), "dd")
 * 
 */

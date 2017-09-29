package test;

import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

public class TestHiveConf2 extends GenericUDTF {
	String shopid = null;
	@Override
	public void close() throws HiveException {
		// TODO Auto-generated method stub
	}
	
    @Override
    public void configure(MapredContext context) {
        Configuration conf = context.getJobConf();
        shopid = conf.get("shopid");
    }
	

	@Override
	public StructObjectInspector initialize(ObjectInspector[] args)
			throws UDFArgumentException {
		// TODO Auto-generated method stub
		 
		 ArrayList<String> fieldNames = new ArrayList<String>();
		 ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
		 fieldNames.add("itemId");
		 fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		 
		 fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		 fieldNames.add("execeptInfo");
		 fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
		 return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames,fieldOIs);
		
	}

	@Override
	public void process(Object[] args) throws HiveException {
		// TODO Auto-generated method stub 
		Long itemId = Long.parseLong(args[0].toString());
		String shopID = args[1].toString();
		shopID = this.shopid;
		forward(new String[]{itemId.toString(), shopID.toString()});
	}
	
	
}

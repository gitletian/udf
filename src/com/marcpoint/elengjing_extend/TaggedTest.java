package com.marcpoint.elengjing_extend;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;


public class TaggedTest {

	public static Map<Long,HashMap<Long, HashMap<String, HashMap<String, String>>>> attrAllDic = null;
	TaggedTool_e taggedTool = new TaggedTool_e();
	String[] separator = new String[] {" && ", ": "};
//	String[] separator = new String[] {";", ":"};
	
	
	
//	String[] separator = new String[] {" && ", ": "};
//	String[] separator = new String[] {";", ":"};
	
	public void process(Object[] args){
		// TODO Auto-generated method stub
		Long itemId = null;
		String message = null;
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
										parasItem = taggedTool.parasItemName(itemId, itemName, attrDic);
										
									}
									
									if( null != parasItem && !parasItem.isEmpty()){
										System.out.println("==itemId="+itemId+"==shopId="+shopID+"==categoryID="+categoryID
												+"; 上市季节="+parasItem.get("上市季节")
												+"; 充绒量="+parasItem.get("充绒量")
												+"; 厚薄="+parasItem.get("厚薄")
												+"; 含绒量="+parasItem.get("含绒量")
												+"; 图案="+parasItem.get("图案")
												+"; 图案文化="+parasItem.get("图案文化")
												+"; 填充物="+parasItem.get("填充物")
												+"; 女装分类="+parasItem.get("女装分类")
												+"; 工艺="+parasItem.get("工艺")
												+"; 廓形="+parasItem.get("廓形")
												+"; 成分含量="+parasItem.get("成分含量")
												+"; 摆型="+parasItem.get("摆型")
												+"; 材质成分="+parasItem.get("材质成分")
												+"; 款式="+parasItem.get("款式")
												+"; 版型="+parasItem.get("版型")
												+"; 牛仔面料盎司="+parasItem.get("牛仔面料盎司")
												+"; 礼服摆型="+parasItem.get("礼服摆型")
												+"; 穿着方式="+parasItem.get("穿着方式")
												+"; 组合形式="+parasItem.get("组合形式")
												+"; 腰型="+parasItem.get("腰型")
												+"; 衣门襟="+parasItem.get("衣门襟")
												+"; 袖型="+parasItem.get("袖型")
												+"; 袖长="+parasItem.get("袖长")
												+"; 裙型="+parasItem.get("裙型")
												+"; 裙长="+parasItem.get("裙长")
												+"; 裤型="+parasItem.get("裤型")
												+"; 裤长="+parasItem.get("裤长")
												+"; 襟形="+parasItem.get("襟形")
												+"; 适用对象="+parasItem.get("适用对象")
												+"; 适用年龄="+parasItem.get("适用年龄")
												+"; 里料="+parasItem.get("里料")
												+"; 销售渠道类型="+parasItem.get("销售渠道类型")
												+"; 面料="+parasItem.get("面料")
												+"; 领子="+parasItem.get("领子")
												+"; 风格="+parasItem.get("风格")
										);
										
										
									}else{
										message = "paras is null";
									}
									
								}else{
									message = "itemAttrDesc is null and itemName is null";
								}
							} catch (Exception e) {
								String ss = itemId == null ? "":itemId.toString();
								StringWriter sw = new StringWriter();
								PrintWriter pw = new PrintWriter(sw);
								e.printStackTrace(pw);
//								forward(new String[]{ss, null, null, "unknow error=="+sw.toString()});
								System.out.println("==itemId="+ss+"; error="+sw.toString());
							}
							
						}else{
							message = "categoryID error ,categoryID not in attrDic ";
						}
					}else{
						message = "itemId or categoryID is null";
					}
					
				}else{
					message = "param is error,must have 4";
				}
			}else{
				message = "attrAllDic file is null";
			}
			
			if(null != message){
				if(null == itemId){
					System.out.println(message);
				}else{
					System.out.println(message);
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			String ss = itemId == null ? "":itemId.toString();
			StringWriter sw = new StringWriter();
			PrintWriter pw = new PrintWriter(sw);
			e.printStackTrace(pw);
			System.out.println("==itemId="+ss+"; error="+sw.toString());
		}
	}
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		TaggedTest testJava = new TaggedTest();
		// TODO Auto-generated method stub
		String itemId = "556052958052";
		String shopId = "71596674";
		String categoryId = "50011277";
		String itemName = "洛可可2017秋装新款时尚双色拼接刺绣夹克棒球服短外套女潮YSK";
		String itemAttrDesc = "材质成分: 聚酯纤维100% && 销售渠道类型: 商场同款(线上线下都销售) && 风格: 甜美 && 货号: 1641WT771 && 服装版型: 直筒 && 厚薄: 常规 && 品牌: Rococo/洛可可 && 组合形式: 单件 && 衣长: 常规 && 袖长: 长袖 && 领子: 立领 && 袖型: 常规 && 衣门襟: 拉链 && 图案: 动物图案 && 流行元素/工艺: 绣花 链条 口袋 螺纹 拉链 拼接 && 适用年龄: 25-29周岁 && 年份季节: 2017年秋季 && 颜色分类: 绿色 粉色 && 尺码: S M L XL";
//		itemAttrDesc = "";
//		itemName= "";
		String[] paramresource = new String[]{itemId, categoryId, shopId, itemName, itemAttrDesc};
		
		
   	 	String industryConfFilePath = "/Users/guoyuanpei/workspace/jworkspace/udf/udf/source/industryattr.json";
		String attrJsonStr = TaggedTool_e.getAttrDictString(industryConfFilePath);
		attrAllDic = TaggedTool_e.paraAttrDict(attrJsonStr);
		
		System.out.println("===============================begin==============================");
		if(!attrAllDic.isEmpty()){
			testJava.process(paramresource);
		}else{
			System.out.println("attrAllDic is empty!");
			
		}
	}
}




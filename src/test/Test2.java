package test;

import java.util.ArrayList;  
import java.util.List;  
  
import org.apache.hadoop.hive.ql.exec.UDAF;  
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;  
  
/** 
 * 计算商户的平均价格 
 * @author xinchun.wang 
 * 
 * 1、函数继承 UDAF 类
 * 2、功能类 的 内部类 实现 UDAFEvaluator 接口
 */  
public final class Test2 extends UDAF {  
  
    /** 
     * The internal state of an aggregation for average. 
     *  
     * Note that this is only needed if the internal state cannot be represented 
     * by a primitive. 
     *  
     * The internal state can also contains fields with types like 
     * ArrayList<String> and HashMap<String,Double> if needed. 初始化点评的平均价格列表 
     */  
    public static class UDAFAvgPriceState {  
        private List<String> oldPriceList = new ArrayList<String>(); 
  
    }
  
    /** 
     * The actual class for doing the aggregation. Hive will automatically look 
     * for all internal classes of the UDAF that implements UDAFEvaluator. 
     * UDAFEvaluator 类 必须实现一下方法
     * 1、 init函数实现接口UDAFEvaluator的init函数
     * 2、 iterate接收传入的参数，并进行内部的轮转。其返回类型为boolean。
     * 3、 terminatePartial无参数，其为iterate函数轮转结束后，返回轮转数据，terminatePartial类似于hadoop的Combiner。
     * 4、 merge接收terminatePartial的返回结果，进行数据merge操作，其返回类型为boolean。
     * 5、 terminate返回最终的聚集函数结果。
     */  
    public static class UDAFAvgPriceEvaluator implements UDAFEvaluator {
  
        UDAFAvgPriceState state;  
  
        public UDAFAvgPriceEvaluator() {  
            super();  
            state = new UDAFAvgPriceState();  
            init();  
        }  
  
        /** 
         * Reset the state of the aggregation. 
         */  
        public void init() {  
            state.oldPriceList = new ArrayList<String>();  
        }  
  
        /** 
         * Iterate through one row of original data. 
         *  
         * The number and type of arguments need to the same as we call this 
         * UDAF from Hive command line. 
         *  
         * This function should always return true. 
         */  
        public boolean iterate(String avgPirce) {  
            if (avgPirce != null) {  
            	state.oldPriceList.add(avgPirce);  
              
            }  
            return true;  
        }  
  
        /** 
         * Terminate a partial aggregation and return the state. If the state is 
         * a primitive, just return primitive Java classes like Integer or 
         * String. 
         */  
        public UDAFAvgPriceState terminatePartial() {  
            // This is SQL standard - average of zero items should be null.  
            return state.oldPriceList == null ? null : state;  
        }  
  
        /** 
         * Merge with a partial aggregation. 
         *  
         * This function should always have a single argument which has the same 
         * type as the return value of terminatePartial(). 
         *  
         * 合并点评平均价格列表 
         */  
        public boolean merge(UDAFAvgPriceState o) {  
            if (o != null) {  
                state.oldPriceList.addAll(o.oldPriceList);  
            }  
            return true;  
        }  
  
        /** 
         * Terminates the aggregation and return the final result. 计算并返回商户平均价格 
         */  
        public Integer terminate() {  
            // This is SQL standard - average of zero items should be null.  
            Integer avgPirce = state.oldPriceList.size() ;
            return avgPirce;
        }  
    }  
  
    private Test2() {  
        // prevent instantiation  
    }  
  
} 


//SELECT  
//A.shopid,  
//calc_avgprice(A.avgprice, A.addtime) AS shop_avgprice,  
//'$cal_dt' as hp_statdate  
//FROM dpstg_credit_shop_avgprice_review_list A  
//INNER JOIN dpstg_credit_shop_tuan_avgprice B ON A.shopid = B.shopid  
//WHERE B.no_tuan_review_count >= 5 AND A.tuan_review = 0  
//GROUP BY A.shopid 

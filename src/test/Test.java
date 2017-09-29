package test;

import java.util.ArrayList;  
import java.util.List;  
  
import org.apache.hadoop.hive.ql.exec.UDAF;  
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;  
  
/** 
 * 计算商户的平均价格 
 * @author xinchun.wang 
 * 
 */  
public final class Test extends UDAF {  
  
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
        private List<Integer> oldPriceList = new ArrayList<Integer>();  
        private List<Integer> newPriceList = new ArrayList<Integer>();  
  
    }  
  
    /** 
     * The actual class for doing the aggregation. Hive will automatically look 
     * for all internal classes of the UDAF that implements UDAFEvaluator. 
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
            state.oldPriceList = new ArrayList<Integer>();  
            state.newPriceList = new ArrayList<Integer>();  
        }  
  
        /** 
         * Iterate through one row of original data. 
         *  
         * The number and type of arguments need to the same as we call this 
         * UDAF from Hive command line. 
         *  
         * This function should always return true. 
         */  
        public boolean iterate(Integer avgPirce, Integer old) {  
            if (avgPirce != null) {  
                if (old == 1)  
                    state.oldPriceList.add(avgPirce);  
                else  
                    state.newPriceList.add(avgPirce);  
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
            return (state.oldPriceList == null && state.newPriceList == null) ? null  
                    : state;  
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
                state.newPriceList.addAll(o.newPriceList);  
            }  
            return true;  
        }  
  
        /** 
         * Terminates the aggregation and return the final result. 计算并返回商户平均价格 
         */  
        public Integer terminate() {  
            // This is SQL standard - average of zero items should be null.  
            Integer avgPirce = 0;  
//            if (state.oldPriceList.size() >= 8  
//                    && state.newPriceList.size() >= 12) {  
//                avgPirce = (CalcAvgPriceUtil.calcInterquartileMean(state.oldPriceList) * 2   
//                        + CalcAvgPriceUtil.calcInterquartileMean(state.newPriceList) * 8) / 10;  
//            } else {  
//                state.newPriceList.addAll(state.oldPriceList);  
//                avgPirce = CalcAvgPriceUtil.calcInterquartileMean(state.newPriceList);  
//            }  
//            avgPirce = 
            return avgPirce == 0 ? null : avgPirce;  
        }  
    }  
  
    private Test() {  
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

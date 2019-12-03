package hadoop.cyril;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @author ：Cyril
 * @date ：Created in 2019/12/3 22:29
 * @description：  Hive 用户自定义函数，根据输入股票参数代码判断是上证深证或者是创业板
 * @modified By：
 */

/**
 * 实现HIVE用户自定义函数继承UDF类，并重载实现evaluate方法
 */
public class StockUDF extends UDF {

    /**
     * @param str 输入参数为函数输入参数，可以为不同类型
     * @return 输出参数为用户自定义类型()
     */
    public String evaluate(String str){
        if(str.length()==0 || str == null) return "代码错误";
        String word = str.substring(0,1);
        switch(word){
            case "0":
                return "深证中小板";
            case "3":
                return "深证创业板";
            case "6":
                return "上证主板";
            default:
                return "代码错误";
        }
    }
}

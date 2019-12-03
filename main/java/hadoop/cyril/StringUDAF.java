package hadoop.cyril;

import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;


/**
 * @author ：Cyril
 * @date ：Created in 2019/12/2 20:44
 * @description： 自定义聚合函数(UDAF) ,求某一列字符串所有数据的长度和
 * @modified By：
 */


/**
 * Hive UDAF需要继承两个重要的类AbstractGenericUDAFResolver,GenericUDAFEvaluator
 */
public class StringUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        /**
         * 判断输入参数长度，输入参数的长度即是函数参数长度
         */
        if(info.length!=1){
            throw new UDFArgumentLengthException("Only one Argument support here");
        }
        /**
         * ObjectInspector ObjectInspector实例代表了一个类型的数据在内存中存储的特定类型和方法
         */
        ObjectInspector io = TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(info[0]);

        /**
         * 判断是否为原始类型，如果不是原始数据类型抛出异常
         */
        if(io.getCategory()!= ObjectInspector.Category.PRIMITIVE){
            throw new UDFArgumentTypeException(0,"Only PRIMITIVE type support here");
        }
        /**
         * PrimitiveObjectInspector 实现ObjectInspector接口，此类里面枚举了所有原始数据类型
         */
        PrimitiveObjectInspector in = (PrimitiveObjectInspector) io;

        /**
         * 判断输入参数是否为原始STRING类型，如果是则返回为String类型实现的StringEvaluator实例，否则抛出异常
         */
        switch (in.getPrimitiveCategory()){
            case STRING:
                return new StringEvaluator();
            default:
                throw new UDFArgumentTypeException(0,"Only String support here,"+((PrimitiveObjectInspector)info[0]).getPrimitiveCategory()+"is passed");
        }
    }

    /**
     * 继承GenericUDAFEvaluator类，并重写其中的方法。
     */
    public static class StringEvaluator extends GenericUDAFEvaluator{

        /**
         * 定义每个阶段输入参数的类型
         */
        private PrimitiveObjectInspector input;
        private PrimitiveObjectInspector output;

        /**
         * 定义函数输出类型
         */
        private ObjectInspector allout;
        private long total;

        /**
         * 存放中间结果的类，需要实现AggregationBuffer接口
         */
        public static class StringAggregation implements AggregationBuffer{
            long sum=0;
            void add(long num){
                this.sum += num;
            }
        }

        /**
         *
         * @param m  枚举类型Mode
         *           PARTIAL1: 这个是mapreduce的map阶段:从原始数据到部分数据聚合 将会调用iterate()和terminatePartial()
         *           PARTIAL2: 这个是mapreduce的map端的Combiner阶段，负责在map端合并map的数据::从部分数据聚合到部分数据聚合: 将会调用merge() 和 terminatePartial()
         *           FINAL: mapreduce的reduce阶段:从部分数据的聚合到完全聚合 将会调用merge()和terminate()
         *           COMPLETE: 如果出现了这个阶段，表示mapreduce只有map，没有reduce，所以map端就直接出结果了:从原始数据直接到完全聚合 将会调用 iterate()和terminate()
         * @param parameters
         * @return
         * @throws HiveException
         */
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            //判断输入参数长度是否为1
            assert (parameters.length==1);
            super.init(m, parameters);
            /**
             * 可以根据不同阶段，定义每个阶段的输入输出参数类型
             */
            if(m==Mode.PARTIAL1 || m==Mode.COMPLETE){
                //当m为Mode.PARTIAL1 或 Mode.COMPLETE 输入参数为String类型
                input = (PrimitiveObjectInspector) parameters[0];
            }else{
                //m为Mode.PARTIAL1 或 Mode.COMPLETE 之外，输入参数为Long类型
                output = (PrimitiveObjectInspector) parameters[0];
            }
            //每个阶段的输出参数都是Long类型
            allout = ObjectInspectorFactory.getReflectionObjectInspector(Long.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
            return allout;
        }

        /**
         * 保存结果的类
         */
        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            StringAggregation agg  = new StringAggregation();
            agg.sum = 0;
            return agg;
        }

        /**
         * @description 重置中间结果
         * @param aggregationBuffer
         * @throws HiveException
         */
        @Override
        public void reset(AggregationBuffer aggregationBuffer) throws HiveException {
            StringAggregation agg  = (StringAggregation)aggregationBuffer;
            agg.sum=0;
        }

        /**
         * @description
         * @param aggregationBuffer 保存中间结果的类
         * @param objects 输入参数
         * @throws HiveException
         */
        @Override
        public void iterate(AggregationBuffer aggregationBuffer, Object[] objects) throws HiveException {
            assert (objects.length==1);
            /**
             * 注意此处的格式转换
             */
//            input = (StringObjectInspector) objects[0];
            Object obj = input.getPrimitiveJavaObject(objects[0]);
            StringAggregation agg = (StringAggregation) aggregationBuffer;
            agg.add(String.valueOf(obj).length());
        }

        @Override
        public Object terminatePartial(AggregationBuffer aggregationBuffer) throws HiveException {
            StringAggregation myagg = (StringAggregation) aggregationBuffer;
            total += myagg.sum;
            return total;
        }

        /**
         * 合并中间结果
         * @param aggregationBuffer
         * @param o
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer aggregationBuffer, Object o) throws HiveException {
            if(o!=null){
                long len = (Long) output.getPrimitiveJavaObject(o);
                StringAggregation agg = (StringAggregation) aggregationBuffer;
                StringAggregation agg1 = new StringAggregation();
                agg1.add(len);
                agg.add(agg1.sum);
            }
        }

        /**
         * 输出最终结果
         * @param aggregationBuffer
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminate(AggregationBuffer aggregationBuffer) throws HiveException {
            StringAggregation agg = (StringAggregation) aggregationBuffer;
            total += agg.sum;
            return total;
        }
    }

}

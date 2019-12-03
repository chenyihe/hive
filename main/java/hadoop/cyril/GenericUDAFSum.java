package hadoop.cyril;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

/**
 * @author ：Cyril
 * @date ：Created in 2019/11/28 19:56
 * @description：
 * @modified By：
 */
public class GenericUDAFSum extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(GenericUDAFSum.class.getName());

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws UDFArgumentTypeException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1,
                    "Exactly one argument is expected.");
        }

        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0,
                    "Only primitive type arguments are accepted but "
                            + parameters[0].getTypeName() + " is passed.");
        }
        switch (((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
            case TIMESTAMP:
                return new org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum.GenericUDAFSumLong();
            case FLOAT:
            case DOUBLE:
            case STRING:
                return new org.apache.hadoop.hive.ql.udf.generic.GenericUDAFSum.GenericUDAFSumDouble();
            case BOOLEAN:
            default:
                throw new UDFArgumentTypeException(0,
                        "Only numeric or string type arguments are accepted but "
                                + parameters[0].getTypeName() + " is passed.");
        }
    }

    public static class GenericUDAFSumLong extends GenericUDAFEvaluator {
        private PrimitiveObjectInspector inputOI;
        private LongWritable result;

        //这个方法返回了UDAF的返回类型，这里确定了sum自定义函数的返回类型是Long类型

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);
            result = new LongWritable(0);
            inputOI = (PrimitiveObjectInspector) parameters[0];
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        /** 存储sum的值的类 */
        static class SumLongAgg implements AggregationBuffer {
            boolean empty;
            long sum;
        }

        //创建新的聚合计算的需要的内存，用来存储mapper,combiner,reducer运算过程中的相加总和。

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumLongAgg result = new SumLongAgg();
            reset(result);
            return result;
        }
        //mapreduce支持mapper和reducer的重用，所以为了兼容，也需要做内存的重用。

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SumLongAgg myagg = (SumLongAgg) agg;
            myagg.empty = true;
            myagg.sum = 0;
        }

        private boolean warned = false;

        //map阶段调用，只要把保存当前和的对象agg，再加上输入的参数，就可以了。
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            try {
                merge(agg, parameters[0]);
            } catch (NumberFormatException e) {
                if (!warned) {
                    warned = true;
                    LOG.warn(getClass().getSimpleName() + " "
                            + StringUtils.stringifyException(e));
                }
            }
        }
        //mapper结束要返回的结果，还有combiner结束返回的结果

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        //combiner合并map返回的结果，还有reducer合并mapper或combiner返回的结果。
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                SumLongAgg myagg = (SumLongAgg) agg;
                myagg.sum += PrimitiveObjectInspectorUtils.getLong(partial, inputOI);
                myagg.empty = false;
            }
        }

        //reducer返回结果，或者是只有mapper，没有reducer时，在mapper端返回结果。
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SumLongAgg myagg = (SumLongAgg) agg;
            if (myagg.empty) {
                return null;
            }
            result.set(myagg.sum);
            return result;
        }
    }

}
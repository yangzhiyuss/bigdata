package com.sky.hive.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.math.BigDecimal;

@Description(
        name = "sum_bignum", value = "_FUNC_(x) - Returns the sum of a set of string"
)

public class GenericUDAFSumBigNum extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(GenericUDAFSumBigNum.class.getName());

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {

        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
        } else if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
        } else if (((PrimitiveTypeInfo)parameters[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "Only string type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
        }

        return  new GenericUDAFSumBigNum.GenericUDAFSumString();
    }

    public static class GenericUDAFSumString extends GenericUDAFEvaluator {
        private transient ObjectInspector inputOI;
        private transient ObjectInspector outputOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);
            inputOI = parameters[0];
            //确定输出类型
            outputOI = ObjectInspectorUtils.getStandardObjectInspector(inputOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
            return outputOI;
        }

        //对类型不进行预估
        @AggregationType(estimable = false)
        static class SumBigNumBuffer extends AbstractAggregationBuffer {
            BigDecimal sum;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            SumBigNumBuffer myAgg = new SumBigNumBuffer();
            reset(myAgg);
            return myAgg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SumBigNumBuffer myAgg = (SumBigNumBuffer) agg;
            myAgg.sum = null;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            merge(agg, parameters[0]);
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            return terminate(agg);
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            //参数为0直接跳出
            if (partial != null) {
                //进行转化
                BigDecimal param = new BigDecimal(toString(partial, inputOI));
                SumBigNumBuffer myAgg = (SumBigNumBuffer) agg;
                if (myAgg.sum ==null) {
                    //初始值为空，直接进行赋值
                    myAgg.sum = param;
                } else {
                    //进行相加
                    myAgg.sum = myAgg.sum.add(param);
                }
            }
        }

        public static String toString(Object o, ObjectInspector oi) {
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
            return poi.getPrimitiveJavaObject(o).toString();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            SumBigNumBuffer myAgg = (SumBigNumBuffer) agg;
            return myAgg.sum.stripTrailingZeros().toPlainString();
        }
    }
}

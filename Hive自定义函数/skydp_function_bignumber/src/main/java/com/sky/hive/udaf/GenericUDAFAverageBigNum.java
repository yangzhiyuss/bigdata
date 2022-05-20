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
import org.codehaus.janino.Java;

import java.math.BigDecimal;

@Description(
        name = "avg_bignum", value = "_FUNC_(x) - Returns the sum of a set of string"
)

public class GenericUDAFAverageBigNum extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(GenericUDAFAverageBigNum.class.getName());

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
        } else if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
        } else if (((PrimitiveTypeInfo)parameters[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "Only string type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
        }

        return  new GenericUDAFAverageBigNum.GenericUDAFAverageString();
    }

    public static class GenericUDAFAverageString extends GenericUDAFEvaluator {
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
        static class AvgBigNumBuffer extends AbstractAggregationBuffer {
            BigDecimal sum;
            long count;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AvgBigNumBuffer myAgg = new AvgBigNumBuffer();
            reset(myAgg);
            return myAgg;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            AvgBigNumBuffer avgBigNumber = (AvgBigNumBuffer) agg;
            avgBigNumber.sum = null;
            avgBigNumber.count = 0;
        }

        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            assert (parameters.length == 1);
            AvgBigNumBuffer myAgg = (AvgBigNumBuffer) agg;
            Object partial = parameters[0];

            if (partial != null) {
                //进行转化
                BigDecimal param = new BigDecimal(toString(partial, inputOI));
                if (myAgg.sum == null) {
                    myAgg.sum = param;
                } else {
                    myAgg.sum = myAgg.sum.add(param);
                }
            }
            //只要进行一次merge,count就加一
            myAgg.count ++;
        }

        public static String toString(Object o, ObjectInspector oi) {
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
            return poi.getPrimitiveJavaObject(o).toString();
        }

        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            AvgBigNumBuffer myAgg = (AvgBigNumBuffer) agg;
            return  myAgg.sum.toString() + "-" + myAgg.count;
        }

        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            if (partial != null) {
                //进行转化
                AvgBigNumBuffer myAgg = (AvgBigNumBuffer) agg;
                String param = toString(partial, inputOI);
                String[] params = param.split("-");
                BigDecimal sumParam = new BigDecimal(params[0]);
                Long countParam = new Long(params[1]);
                if (myAgg.sum == null) {
                    myAgg.sum = sumParam;
                    myAgg.count = countParam;
                } else {
                    myAgg.sum = myAgg.sum.add(sumParam);
                    myAgg.count += countParam;
                }
            }
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            AvgBigNumBuffer myAgg = (AvgBigNumBuffer) agg;
            if (myAgg.count == 0) {
                return null;
            }
            BigDecimal countBigDecimal = new BigDecimal(myAgg.count);
            //return myAgg.sum.divide(countBigDecimal, 18, BigDecimal.ROUND_HALF_UP).toString();
            return myAgg.sum.divide(countBigDecimal, 18, BigDecimal.ROUND_HALF_UP).stripTrailingZeros().toPlainString();
        }
    }
}

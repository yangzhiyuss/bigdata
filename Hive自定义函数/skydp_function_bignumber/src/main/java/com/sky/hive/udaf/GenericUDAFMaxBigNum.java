package com.sky.hive.udaf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.math.BigDecimal;

@Description(
        name = "max_bignum", value = "_FUNC_(x) - Returns the sum of a set of string"
)

public class GenericUDAFMaxBigNum extends AbstractGenericUDAFResolver {
    static final Log LOG = LogFactory.getLog(GenericUDAFMaxBigNum.class.getName());

    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        if (parameters.length != 1) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Exactly one argument is expected.");
        } else if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Only primitive type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
        } else if (((PrimitiveTypeInfo)parameters[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
            throw new UDFArgumentTypeException(0, "Only string type arguments are accepted but " + parameters[0].getTypeName() + " is passed.");
        }

        return  new GenericUDAFMaxString();
    }

    @UDFType(distinctLike=true)
    public static class GenericUDAFMaxString extends GenericUDAFEvaluator {
        private transient ObjectInspector inputOI;
        private transient ObjectInspector outputOI;

        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            assert (parameters.length == 1);
            super.init(m, parameters);
            inputOI = parameters[0];

            outputOI = ObjectInspectorUtils.getStandardObjectInspector(inputOI,
                    ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
            return outputOI;
        }

        static class MaxBigNumBuffer extends AbstractAggregationBuffer {
            Object o;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            return new MaxBigNumBuffer();
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            MaxBigNumBuffer maxBigNumBuffer = (MaxBigNumBuffer)agg;
            maxBigNumBuffer.o = null;
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
                MaxBigNumBuffer myAgg = (MaxBigNumBuffer) agg;

                if (myAgg.o ==null) {
                    //初始值为空，直接进行赋值
                    myAgg.o = ObjectInspectorUtils.copyToStandardObject(partial, inputOI,
                            ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                } else {
                    //比较两个数的大小
                    BigDecimal result = new BigDecimal(toString(myAgg.o, outputOI));
                    //大值进行赋值
                    if (result.compareTo(param) < 0) {
                        myAgg.o = ObjectInspectorUtils.copyToStandardObject(partial, inputOI,
                                ObjectInspectorUtils.ObjectInspectorCopyOption.JAVA);
                    }
                }
            }
        }

        public static String toString(Object o, ObjectInspector oi) {
            PrimitiveObjectInspector poi = (PrimitiveObjectInspector)oi;
            return poi.getPrimitiveJavaObject(o).toString();
        }

        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            MaxBigNumBuffer myAgg = (MaxBigNumBuffer) agg;
            return myAgg.o;
        }
    }
}

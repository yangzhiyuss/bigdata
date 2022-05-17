import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.math.BigDecimal;

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

@Description(
        name = "mult_bignum",
        value = "Returns the multiply value of string"
)

public class GenericUDFMultiplyBigNum extends GenericUDF {
    private transient ObjectInspectorConverters.Converter[] converters = new ObjectInspectorConverters.Converter[2];
    private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[2];
    private final Text output = new Text();

    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        //检查输入参数的长度
        checkArgsSize(objectInspectors, 2, 2);

        //检查输入参数是否是基本类型
        checkArgPrimitive(objectInspectors, 0);
        checkArgPrimitive(objectInspectors, 1);

        //检查输入类型是否是string类型
        checkArgGroups(objectInspectors, 0, inputTypes, STRING_GROUP);
        checkArgGroups(objectInspectors, 1, inputTypes, STRING_GROUP);

        //获取类型的转换机制
        obtainStringConverter(objectInspectors, 0, inputTypes, converters);
        obtainStringConverter(objectInspectors, 1, inputTypes, converters);

        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }

    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        BigDecimal num1 = this.transferArgToBigDecimal(deferredObjects, 0);
        BigDecimal num2 = this.transferArgToBigDecimal(deferredObjects, 1);

        output.set(num1.multiply(num2).toString());
        return output;
    }

    private BigDecimal transferArgToBigDecimal(DeferredObject[] arguments, int position) throws HiveException {
        BigDecimal num;

        String arg = getStringValue(arguments, position, converters);
        //判断输入的字符串是否为空
        if (arg == null || "".equals(arg)) {
            //输入的sting为空，转化为0
            num = new BigDecimal("0");
        } else {
            num = new BigDecimal(arg);
        }
        return num;
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }
}

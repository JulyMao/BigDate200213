package com.me;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * @author maow
 * @create 2020-04-28 20:28
 */
public class My_UDF extends GenericUDF {
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        if (objectInspectors == null || objectInspectors.length !=1){
            throw new UDFArgumentLengthException("Input args number error");
        }
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)){
            throw new UDFArgumentTypeException(0,"Input args type error");
        }
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }

    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        if(deferredObjects[0].get() == null){
            return 0 ;
        }
        return deferredObjects[0].get().toString().length();
    }

    public String getDisplayString(String[] strings) {
        return "";
    }
}

package com.me;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author maow
 * @create 2020-04-28 20:45
 */
public class My_UDTF extends GenericUDTF {
    private ArrayList<String> outList = new ArrayList<String>();
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<String> fieldsNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldsNames.add("word");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldsNames,fieldOIs);
    }

    public void process(Object[] objects) throws HiveException {
        String arg = objects[0].toString();
        String splitKey = objects[1].toString();
        String[] strings = arg.split(splitKey);
        for (String string : strings) {
            outList.clear();
            outList.add(string);
            forward(outList);
        }
    }

    public void close() throws HiveException {

    }
}

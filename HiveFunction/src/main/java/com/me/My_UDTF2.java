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
 * @create 2020-04-28 21:00
 */
public class My_UDTF2 extends GenericUDTF {

    /**
     *  将hello_world,mao_wei,java_hive切成
     *
     *  hello   world
     *  mao     wei
     *  java    hive
     *
     */

    private ArrayList<String> outList = new ArrayList<String>();
    @Override
    public StructObjectInspector initialize(StructObjectInspector argOIs) throws UDFArgumentException {
        List<String> fieldsNames = new ArrayList<String>();
        List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        fieldsNames.add("word1");
        fieldsNames.add("word2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldsNames,fieldOIs);
    }

    public void process(Object[] objects) throws HiveException {
        String arg = objects[0].toString();
        String splitKey1 = objects[1].toString();
        String splitKey2 = objects[2].toString();
        String[] strings = arg.split(splitKey1);
        for (String string : strings) {
            outList.clear();
            String[] splits = string.split(splitKey2);
            for (String split : splits) {
                outList.add(split);
            }
//            outList.add(string);
            forward(outList);
        }
    }

    public void close() throws HiveException {

    }
}

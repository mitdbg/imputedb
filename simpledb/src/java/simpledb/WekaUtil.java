package simpledb;

import java.util.ArrayList;
import java.util.List;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class WekaUtil {
    
    /**
     * Create a Weka Attribute corresponding to the name of the field at index
     * `index` of the TupleDesc.
     * @param td TupleDesc
     * @param index index of field
     * @return the new Attribute
     */
    public static Attribute fieldToAttribute(TupleDesc td, int index){
        String name = td.getFieldName(index);
        Type type = td.getFieldType(index);
        
        return fieldToAttribute(name, type);
    }

    /**
     * Create a Weka Attribute with name `name`.
     * @param name name of attribute
     * @param type type of attribute. This is currently only required to enforce
     * the *lack* of String support.
     * @return the new Attribute
     */
    public static Attribute fieldToAttribute(String name, Type type){
        if (!(type == Type.INT_TYPE || type == Type.DOUBLE_TYPE)){
            throw new UnsupportedOperationException();
        }
        
        return new Attribute(name);
    }

    /**
     * Create a list of Weka Attributes from a TupleDesc. The resulting list is
     * suitable to pass to an Instances object.
     * @param td the TupleDesc
     * @return the list of Attributes
     */
    public static ArrayList<Attribute> tupleDescToAttributeList(TupleDesc td){
        List<Integer> fields = new ArrayList<>();
        for (int i=0; i<td.numFields(); i++){
            fields.add(i);
        }
        return tupleDescToAttributeList(td, fields);
    }

    /**
     * Create a list of Weka Attributes from a TupleDesc. The resulting list is
     * suitable to pass to an Instances object. This does no validation on `fields`.
     * @param td the TupleDesc
     * @param fields indices identifying which fields should be included
     * @return the list of Attributes
     */
    public static ArrayList<Attribute> tupleDescToAttributeList(TupleDesc td, List<Integer> fields){
        ArrayList<Attribute> attrs = new ArrayList<>(fields.size());
        for (int i : fields){
            Attribute attr = fieldToAttribute(td, i);
            attrs.add(attr);
        }
        
        return attrs;
    }
    
    /**
     * Create an Instances object from the tuples provided. The Instances has
     * name `name` and every value from every tuple. The TupleDesc is provided
     * separately just to validate that all of the provided Tuples share this
     * TupleDesc.
     * @param name the name of the resulting Instances object
     * @param ts list of Tuples
     * @param td TupleDesc
     * @return new Instances object containing the values from all the tuples.
     */
    public static Instances relationToInstances(String name, List<Tuple> ts, TupleDesc td){
        List<Integer> fields = new ArrayList<>();
        for (int i=0; i<td.numFields(); i++){
            fields.add(i);
        }
        return relationToInstances(name, ts, td, fields);
    }

    /**
     * Create an Instances object from the tuples provided. The Instances has
     * name `name` and every value from every tuple. The TupleDesc is provided
     * separately just to validate that all of the provided Tuples share this
     * TupleDesc.
     * @param name the name of the resulting Instances object
     * @param ts list of Tuples
     * @param td TupleDesc
     * @param fields indices identifying which fields should be included in the new Instances object.
     * @return new Instances object containing the values from all the tuples.
     */
    public static Instances relationToInstances(String name, List<Tuple> ts, TupleDesc td,
            List<Integer> fields){
        ArrayList<Attribute> attrs = tupleDescToAttributeList(td, fields);
        int relationSize = ts.size();
        Instances instances = new Instances(name, attrs, relationSize);
        
        for (int i=0; i<ts.size(); i++){
            Tuple t = ts.get(i);
            if (!t.getTupleDesc().equals(td)){
                throw new RuntimeException("All TupleDescs must match.");
            }
            instances.add(i, tupleToInstance(t, attrs, fields));
        }
        
        return instances;
    }

    /**
     * Create a new Instance from the values in Tuple t. We require that a list
     * of Attributes is also passed so that all of the instances have shared
     * references to the same Attributes.
     * @param t Tuple to convert
     * @param attrs Attributes corresponding to fields of Instance
     * @return new Instance with values from Tuple t
     */
    public static Instance tupleToInstance(Tuple t, List<Attribute> attrs){
        List<Integer> fields = new ArrayList<>();
        for (int i=0; i<t.getTupleDesc().numFields(); i++){
            fields.add(i);
        }
        return tupleToInstance(t, attrs, fields);
    }
    
    /**
     * Create a new Instance from the values in Tuple t. We require that a list
     * of Attributes is also passed so that all of the instances have shared
     * references to the same Attributes. No validation is performed against `fields`.
     * @param t Tuple to convert
     * @param attrs Attributes corresponding to fields of Instance
     * @param fields indices identifying which fields should be included in the
     * new Instances object. The ith element of fields must correspond to the
     * ith element of attrs.
     * @return new Instance with values from Tuple t
     */
    public static Instance tupleToInstance(Tuple t, List<Attribute> attrs, List<Integer> fields){
        TupleDesc td = t.getTupleDesc();
        int numFields = fields.size();

        Instance inst = new DenseInstance(numFields);
        
        int index = 0;
        for (int i : fields){
            Type type = td.getFieldType(i);

            if (type == Type.INT_TYPE){
                if (!t.getField(i).isMissing()){
                    int value = ((IntField) t.getField(i)).getValue();
                    // TODO see toDoubleArray
                    inst.setValue(attrs.get(index), value);
                } else {
                    inst.setMissing(index);
                }
            } else if (type == Type.DOUBLE_TYPE){
                if (!t.getField(i).isMissing()){
                    double value = ((DoubleField) t.getField(i)).getValue();
                    inst.setValue(attrs.get(index), value);
                } else {
                    inst.setMissing(index);
                }
            } else {
                throw new UnsupportedOperationException();
            }
            
            index++;
        }
        
        return inst;
    }
    
    /**
     * Create a new Tuple by extracting the values from the Instance inst and using the TupleDesc td
     * @param inst Instance
     * @param td TupleDesc
     * @return new Tuple
     */
    public static Tuple instanceToTuple(Instance inst, TupleDesc td){
        Tuple t = new Tuple(td);
        for (int i=0; i<td.numFields(); i++){
            double value = inst.value(i);
            Type type = td.getFieldType(i);
            Field field = null;
            if (type.equals(Type.INT_TYPE)){
                field = new IntField((int) value);
            } else if (type.equals(Type.DOUBLE_TYPE)){
                field = new DoubleField(value);
            } else if (type.equals(Type.STRING_TYPE)){
                throw new UnsupportedOperationException();
                // field = new StringField(value);
            }
            
            t.setField(i, field);
        }

        return t;
    }

}
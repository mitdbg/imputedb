package simpledb;

import java.util.ArrayList;
import java.util.List;

import weka.core.Attribute;
import weka.core.DenseInstance;
import weka.core.Instance;
import weka.core.Instances;

public class WekaUtil {
	
	public Attribute fieldToAttribute(TupleDesc td, int index){
		String name = td.getFieldName(index);
		Type type = td.getFieldType(index);
		
		return fieldToAttribute(name, type);
		
	}

	public Attribute fieldToAttribute(String name, Type type){
		if (!(type == Type.INT_TYPE || type == Type.DOUBLE_TYPE)){
			throw new UnsupportedOperationException();
		}
		
		return new Attribute(name);
	}

	public List<Attribute> tupleDescToAttributeList(TupleDesc td){
		List<Attribute> attrs = new ArrayList<>(td.numFields());
		for (int i=0; i<td.numFields(); i++){
			if (!(td.getFieldType(i) == Type.INT_TYPE || 
					td.getFieldType(i) == Type.DOUBLE_TYPE)){
				throw new UnsupportedOperationException();
			}

			Attribute attr = new Attribute(td.getFieldName(i));
			attrs.add(i, attr);
		}
		
		return attrs;
	}
	
	public static Instances relationToInstances(String name, List<Tuple> ts, List<Attribute> attrs){
		int relationSize = ts.size();
		Instances instances = new Instances(name, (ArrayList<Attribute>) attrs, relationSize);
		
		for (int i=0; i<ts.size(); i++){
			instances.add(i, tupleToInstance(ts.get(i), attrs));
		}
		
		return instances;
	}
	
	public static Instance tupleToInstance(Tuple t, List<Attribute> attrs){
		TupleDesc td = t.getTupleDesc();
		int numFields = td.numFields();

		Instance inst = new DenseInstance(numFields);
		
		for (int i=0; i<numFields; i++){
			Type type = td.getFieldType(i);

			if (type == Type.INT_TYPE){
				int value = ((IntField) t.getField(i)).getValue();
				inst.setValue(attrs.get(i), value);
			} else if (type == Type.DOUBLE_TYPE){
				double value = ((DoubleField) t.getField(i)).getValue();
				inst.setValue(attrs.get(i), value);
			} else {
				throw new UnsupportedOperationException();
			}
		}
		
		return inst;
	}
	
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

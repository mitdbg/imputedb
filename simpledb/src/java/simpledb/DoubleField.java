package simpledb;

import java.io.DataOutputStream;
import java.io.IOException;

import simpledb.Predicate.Op;

public class DoubleField implements Field {
	private static final long serialVersionUID = -7652884565222581467L;
	
	private final Double value;
	
	public DoubleField(double value) {
		if (value == Type.MISSING_DOUBLE) {
			this.value = null;
		} else {
			this.value = value;
		}
	}
	
	public DoubleField() {
		this.value = null;
	}

	@Override
	public void serialize(DataOutputStream dos) throws IOException {
		if (value == null) {
			dos.writeDouble(Type.MISSING_DOUBLE);
		} else {
			dos.writeDouble(value);
		}
	}

	@Override
	public boolean compare(Op op, Field value) {
		DoubleField val = (DoubleField) value;
		
		if (this.value == null) {
			return op == Predicate.Op.EQUALS && val.isMissing();
		}
		
		switch(op) {
		case LIKE:
		case EQUALS:
			return this.value == val.value;
		case GREATER_THAN:
			return this.value > val.value;
		case GREATER_THAN_OR_EQ:
			return this.value >= val.value;
		case LESS_THAN:
			return this.value < val.value;
		case LESS_THAN_OR_EQ:
			return this.value >= val.value;
		case NOT_EQUALS:
			return this.value != val.value;
		default:
			throw new RuntimeException("Unexpected operator.");
		}
	}

	@Override
	public Type getType() {
		return Type.DOUBLE_TYPE;
	}

	@Override
	public boolean isMissing() {
		return value == null;
	}
	
	public double getValue() {
		return value;
	}
	
	public double getValueDefault(double default_) {
    	if (isMissing()) {
            return default_;
        }
        return value;
    }

	@Override
	public String toString() {
		if (isMissing()) {
			return HeapFileEncoder.NULL_STRING;
		}
		return Double.toString(value);
	}
}

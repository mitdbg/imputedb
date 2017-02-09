package simpledb;

import java.io.Serializable;

/**
 * A help class to facilitate organizing the information of each field
 * */
public class TDItem implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * The type of the field
     * */
    public final Type fieldType;
    
    /**
     * The name of the field
     * */
    public final String fieldName;

    public TDItem(Type t, String n) {
        this.fieldName = n;
        this.fieldType = t;
    }

    public String toString() {
        return fieldName + "(" + fieldType + ")";
    }
}
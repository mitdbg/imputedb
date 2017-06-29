package simpledb;

import java.text.ParseException;
import java.io.*;

/**
 * Class representing a type in SimpleDB.
 * Types are static objects defined by this class; hence, the Type
 * constructor is private.
 */
public enum Type implements Serializable {
    INT_TYPE(4) {
        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                int readValue = dis.readInt();
                if (readValue == MISSING_INTEGER) {
                    return new IntField();
                } else {
                    return new IntField(readValue);
                }
            } catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }
    }, STRING_TYPE(Defaults.STRING_LEN + 4) {
        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                int strLen = dis.readInt();
                byte bs[] = new byte[strLen];
                dis.read(bs);
                dis.skipBytes(STRING_LEN - strLen);
                String readValue = new String(bs);
                if (readValue.equals(MISSING_STRING)) {
                    return new StringField(STRING_LEN);
                } else {
                    return new StringField(readValue, STRING_LEN);
                }
            } catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }
    }, DOUBLE_TYPE(8) {
        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                double val = dis.readDouble();
                if (val == MISSING_DOUBLE) {
                    return new DoubleField();
                } else {
                    return new DoubleField(val);
                }
            } catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }

    };

    private static class Defaults {
        public static final int STRING_LEN = 128;
    }

    private Type(int length) {
        this.length = length;
    }

    public static final int STRING_LEN = Defaults.STRING_LEN;

    /**
     * @return the number of bytes required to store a field of this type.
     */
    public final int length;

    /**
     * @param dis The input stream to read from
     * @return a Field object of the same type as this object that has contents
     * read from the specified DataInputStream.
     * @throws ParseException if the data read from the input stream is not
     *                        of the appropriate type.
     */
    public abstract Field parse(DataInputStream dis) throws ParseException;

    // dummy value for missing integers
    public static final int MISSING_INTEGER = Integer.MIN_VALUE;
    // dummy string for missing strings
    public static final String MISSING_STRING = new String(new char[STRING_LEN]).replace('\0', 'Z');
    public static final double MISSING_DOUBLE = Double.MIN_VALUE;

    @Override
    public String toString() {
        switch (this) {
            case INT_TYPE:
                return "int";
            case DOUBLE_TYPE:
                return "double";
            case STRING_TYPE:
                return "string";
            default:
                throw new RuntimeException("Unexpected type.");
        }
    }

    /**
     * Parse a Type object from a string.
     */
    public static Type ofString(String typeStr) throws ParseException {
        typeStr = typeStr.toLowerCase();

        if (typeStr.equals("int")) {
            return Type.INT_TYPE;
        } else if (typeStr.equals("string")) {
            return Type.STRING_TYPE;
        } else if (typeStr.equals("double")) {
            return Type.DOUBLE_TYPE;
        } else {
            throw new ParseException("Unexpected type: " + typeStr, 0);
        }
    }
}

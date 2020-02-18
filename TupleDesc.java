package simpledb;

import java.awt.*;
import java.io.Serializable;
import java.util.*;
import java.util.List;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

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

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        // some code goes here
        List<TDItem> tdItems = new ArrayList<>(types.length);
        for (int i = 0; i < types.length; i++)
            tdItems.add(new TDItem(types[i], fieldNames[i]));
        return tdItems.iterator();
    }

    private static final long serialVersionUID = 1L;

    Type[] types;
    String[] fieldNames = null;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        types = typeAr;
        fieldNames = fieldAr;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        types = typeAr;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return types.length;
    }


    private boolean validIndex(int i) { return i >= 0 && i < types.length; }

    public String[] getFieldNames() {
        return fieldNames;
    }

    public Type[] getTypes() {
        return types;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        if (!validIndex(i) || fieldNames == null)
            throw new NoSuchElementException();
        return fieldNames[i];
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (!validIndex(i))
            throw new NoSuchElementException();
        return types[i];
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        if (fieldNames != null) {
            String[] fNames = fieldNames;
            for (int i = 0; i < fNames.length; i++) {
                if (fNames[i].equals(name))
                    return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int sum = 0;
        for (Type type : types)
            sum += type.getLen();
        return sum;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        Type[] td1Types = td1.types;
        Type[] td2Types = td2.types;
        Type[] typeAr = new Type[td1Types.length + td2Types.length];
        System.arraycopy(td1Types, 0, typeAr, 0, td1Types.length);
        System.arraycopy(td2Types, 0, typeAr, td1Types.length, td2Types.length);

        String[] fieldAr = null;
        String[] td1FieldsName = td1.fieldNames;
        String[] td2FieldsName = td2.fieldNames;
        if (td1FieldsName != null && td2FieldsName != null) {
            fieldAr = new String[td1FieldsName.length + td2FieldsName.length];
            System.arraycopy(td1FieldsName, 0, fieldAr, 0, td1FieldsName.length);
            System.arraycopy(td2FieldsName, 0, fieldAr, td1FieldsName.length, td2FieldsName.length);
        }
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == this)
            return true;

        if (o instanceof TupleDesc) {
            TupleDesc other = (TupleDesc) o;
            Type[] otherTypes = other.types;
            if (otherTypes.length != types.length)
                return false;
            for (int i = 0; i < otherTypes.length; i++) {
                if (otherTypes[i] != types[i])
                    return false;
            }
            return true;
        }
        return false;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        Type[] typeAr = this.types;
        String[] fieldAr = fieldNames;
        StringBuilder builder = new StringBuilder(typeAr[0].toString()).append("(").append(fieldAr[0]).append(")");
        for (int i = 1; i < typeAr.length; i++)
            builder.append(",").append(typeAr[i].toString()).append("(").append(fieldAr[i]).append(")");
        return builder.toString();
    }
}

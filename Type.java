package simpledb;

import java.text.ParseException;
import java.io.*;

/**
 * Class representing a type in SimpleDB.
 * Types are static objects defined by this class; hence, the Type
 * constructor is private.
 */
public enum Type implements Serializable {
    INT_TYPE() {
        @Override
        public int getLen() {
            return 4;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                return new IntField(dis.readInt());
            }  catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }

        @Override
        public byte[] toBytes(Object o)
        {
            byte[] data = new byte[4];
            integerToBytes(data, (Integer) o);
            return data;
        }

    }, STRING_TYPE() {
        @Override
        public int getLen() {
            return STRING_LEN+4;
        }

        @Override
        public Field parse(DataInputStream dis) throws ParseException {
            try {
                int strLen = dis.readInt();
                byte bs[] = new byte[strLen];
                dis.read(bs);
                dis.skipBytes(STRING_LEN-strLen);
                return new StringField(new String(bs), STRING_LEN);
            } catch (IOException e) {
                throw new ParseException("couldn't parse", 0);
            }
        }

        @Override
        public byte[] toBytes(Object o)
        {
            String val = (String) o;
            byte[] data = new byte[STRING_LEN + 4];
            int length = val.length();
            integerToBytes(data, length);
            byte[] strByes = val.getBytes();
            for (int i = 0; i < length; i++)
                data[i + 4] = strByes[i];
            return data;
        }
    };
    
    public static final int STRING_LEN = 128;

  /**
   * @return the number of bytes required to store a field of this type.
   */
    public abstract int getLen();

  /**
   * @return a Field object of the same type as this object that has contents
   *   read from the specified DataInputStream.
   * @param dis The input stream to read from
   * @throws ParseException if the data read from the input stream is not
   *   of the appropriate type.
   */
    public abstract Field parse(DataInputStream dis) throws ParseException;


    public abstract byte[] toBytes(Object o);

    static private void integerToBytes(byte[] data, int val)
    {
        data[0] = (byte) (val >> 24);
        data[1] = (byte) ((val >> 16) & 0x0ff);
        data[2] = (byte) ((val >> 8) & 0x0ff);
        data[3] = (byte) (val & 0x0ff);
    }
}

package cn.edu.tsinghua.iginx.engine.shared.data.read;

import cn.edu.tsinghua.iginx.thrift.DataType;

public class Batch {
    static public int BATCH_SIZE = 1024*32;
    // just for now
    private double[] doubles;
    private int[] ints;
    private long[] keys;
    private Header header;
    private DataType dataType;
    private byte[] bitmap;

    public Batch() {}

    public Batch(Header header, long[] keys, Object values) {
        this.header = header;
        this.keys = keys;
        if (values instanceof int[]) {
            this.ints = (int[])values;
            this.dataType = DataType.INTEGER;
        } else if (values instanceof double[]) {
            this.doubles = (double[])values;
            this.dataType= DataType.DOUBLE;
        }
    }

    public Batch(Header header, long[] keys) {
        this.header = header;
        this.keys = keys;
    }

    public Object getBatch() {
        switch (dataType) {
            case DOUBLE:
                return doubles;
            case INTEGER:
                return ints;
        }
        return null;
    }

    public DataType getDataType() {
        return dataType;
    }

    public double[] getDoubles() {
        return doubles;
    }

    public int[] getInts() {
        return ints;
    }

    public long[] getKeys() {
        return keys;
    }

    public Header getHeader() {
        return header;
    }

    public void setDoubles(double[] doubles) {
        this.doubles = doubles;
        this.dataType= DataType.DOUBLE;
    }

    public void setInts(int[] ints) {
        this.ints = ints;
        this.dataType= DataType.INTEGER;
    }

    public void setKeys(long[] keys) {
        this.keys = keys;
    }

    public void setHeader(Header header) {
        this.header = header;
    }

    public byte[] getBitmap() {
        return bitmap;
    }

    public void setBitmap(byte[] bitmap) {
        this.bitmap = bitmap;
    }
}

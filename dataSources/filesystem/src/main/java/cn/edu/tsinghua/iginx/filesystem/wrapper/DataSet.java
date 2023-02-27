package cn.edu.tsinghua.iginx.filesystem.wrapper;

import cn.edu.tsinghua.iginx.filesystem.FileSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DataSet {
    private static final Logger logger = LoggerFactory.getLogger(DataSet.class);
    private int MAXDATASETLEN = 100;
    private final byte[] rawData = new byte[MAXDATASETLEN];
    private int index =  0;

    public void addRawDataRecord(byte[] oriData, int beg, int end) {
        for(int i = beg; i<end; i++) {
            rawData[index++] = oriData[i];
            if(index >= MAXDATASETLEN) {

            }
        }
    }

    public String toStringSet() {
        String res = new String();
        for(Byte val : rawData) {
            res += val.toString() + ",";
        }
        res.getBytes()
    }
}

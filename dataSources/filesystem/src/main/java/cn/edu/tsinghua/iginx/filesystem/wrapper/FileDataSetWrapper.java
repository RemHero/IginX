package cn.edu.tsinghua.iginx.filesystem.wrapper;

import cn.edu.tsinghua.iginx.filesystem.tools.SeriesOperator;

import java.util.List;

/*
 * FileDataSetWrapper是文件系统视图向 IGinX 内部视图转换的 Wrapper
 */
public class FileDataSetWrapper {
    private final List<SeriesOperator> seriesList;
    private final List<byte[]> value;
    private int index = 0;
    public FileDataSetWrapper(List<SeriesOperator> pathList, List<byte[]> result) {
        seriesList = pathList;
        value = result;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public boolean hasNext() {
        if (index >= value.size()) return false;
        return true;
    }

    public


}













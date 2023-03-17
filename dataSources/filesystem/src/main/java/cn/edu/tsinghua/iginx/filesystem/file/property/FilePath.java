package cn.edu.tsinghua.iginx.filesystem.file.property;

public final class FilePath {
    private static String SEPARATOR = "/";
    private String oriSeries;
    private String filePath;
    private String fileName;

    public FilePath(String storageUnit, String oriSeries) {
        this.oriSeries = oriSeries;
        convertSeriesToFileSystemPath(storageUnit, oriSeries, SEPARATOR);
    }

    private void convertSeriesToFileSystemPath(String storageUnit, String series, String separator) {
        //之后根据规则修改获取文件名的方法， may fix it
        filePath = storageUnit == null ? "" : storageUnit + separator + series;
//        filePath = storageUnit == null ? "" : storageUnit + separator + series.replace(".", separator);
    }

    public String getOriSeries() {
        return oriSeries;
    }

    public String getFilePath() {
        return filePath;
    }

    public String getFileName() {
        return fileName;
    }
}

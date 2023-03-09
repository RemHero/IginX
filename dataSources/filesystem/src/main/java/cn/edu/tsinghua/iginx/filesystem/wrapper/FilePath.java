package cn.edu.tsinghua.iginx.filesystem.wrapper;

public final class FilePath {
    private static String SEPARATOR = "/";
    private String oriSeries;
    private String filePath;
    private String fileName;

    public FilePath(String storageUnit, String oriSeries) {
        this.oriSeries = oriSeries;
        convertSeriesToFileSystemPath(storageUnit, oriSeries);
    }

    private void convertSeriesToFileSystemPath(String storageUnit, String series) {
        //之后根据规则修改获取文件名的方法， may fix it
        filePath = storageUnit == null ? "":storageUnit + SEPARATOR + series.replace(".", SEPARATOR);
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

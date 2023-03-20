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
        filePath = series == null ? storageUnit : series;//临时逻辑
//        filePath = storageUnit == null ? "" : separator + storageUnit + separator + series;
//        filePath = storageUnit == null ? "" : storageUnit + separator + series.replace(".", separator);
    }

    public static String convertFilePathToSeries(String filePath, String separator) {
        return separator + filePath.replace(separator, ".");
    }

    public static String convertFilePathToSeries(String filePath) {
        return SEPARATOR + filePath.replace(SEPARATOR, ".");
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

    public static void setSeparator(String SEPARATOR) {
        FilePath.SEPARATOR = SEPARATOR;
    }
}

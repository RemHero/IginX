package cn.edu.tsinghua.iginx.filesystem.tools;

public final class SeriesOperator {
    private static String SEPARATOR = "/";
    private String oriSeries;
    private String filePath;
    private String fileName;

    public SeriesOperator(String storageUnit, String oriSeries) {
        this.oriSeries = oriSeries;
        convertSeriesToFileSystemPath(storageUnit, oriSeries);
    }

    private void convertSeriesToFileSystemPath(String storageUnit, String series) {
        filePath = storageUnit + SEPARATOR + series.replace(".", SEPARATOR);
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

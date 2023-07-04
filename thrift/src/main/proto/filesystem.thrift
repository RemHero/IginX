include "common.thrift"
namespace java cn.edu.tsinghua.iginx.filesystem.thrift

struct FSFilter {
    1: required common.FilterType type
    2: optional list<FSFilter> children
    3: optional bool isTrue
    4: optional i64 keyValue
    5: optional common.Op op
    6: optional string pathA
    7: optional string pathB
    8: optional string path
    9: optional common.Value value
}

struct FileDataHeader {
    1: required list<string> names
    2: required list<string> types
    3: required list<map<string, string>> tagsList
    4: required bool hasTime
}

struct FileDataRow {
    1: optional i64 timestamp
    2: required binary rowValues
    3: required binary bitmap
}

struct ProjectReq {
    1: required string storageUnit
    2: required bool isDummyStorageUnit
    3: required list<string> paths
    4: optional common.RawTagFilter tagFilter
    5: optional FSFilter filter
}

struct ProjectResp {
    1: required common.Status status
    2: optional FileDataHeader header
    3: optional list<FileDataRow> rows
}

struct FileDataRawData {
    1: required list<string> paths
    2: required list<map<string, string>> tagsList
    3: required binary timestamps
    4: required list<binary> valuesList
    5: required list<binary> bitmapList
    6: required list<string> dataTypeList
    7: required string rawDataType
}

struct InsertReq {
    1: required string storageUnit
    2: required FileDataRawData rawData;
}

struct FileSystemTimeRange {
    1: required i64 beginTime;
    2: required bool includeBeginTime;
    3: required i64 endTime;
    4: required bool includeEndTime;
}

struct DeleteReq {
    1: required string storageUnit
    2: required list<string> paths
    3: optional common.RawTagFilter tagFilter
    4: optional list<FileSystemTimeRange> timeRanges
}

struct PathSet {
    1: required string path
    2: required string dataType
    3: optional map<string, string> tags
}

struct GetTimeSeriesOfStorageUnitResp {
    1: required common.Status status
    2: optional list<PathSet> PathList
}

service FileSystemService {

    ProjectResp executeProject(1: ProjectReq req);

    common.Status executeInsert(1: InsertReq req);

    common.Status executeDelete(1: DeleteReq req);

    GetTimeSeriesOfStorageUnitResp getTimeSeriesOfStorageUnit(1: string storageUnit);

    common.GetStorageBoundaryResp getBoundaryOfStorage(1: string prefix);

}
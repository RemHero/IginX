package cn.edu.tsinghua.iginx.filesystem.tools;

import cn.edu.tsinghua.iginx.engine.shared.operator.filter.*;
import cn.edu.tsinghua.iginx.utils.JsonUtils;
import com.alibaba.fastjson2.JSON;

import java.util.stream.Collectors;

public class FilterTransformer {
    public static byte[] toString(Filter filter) {
        if (filter == null) {
            return null;
        }
        return JsonUtils.toJson(filter);
    }

    private static String toString(NotFilter filter) {
        return "not " + filter.toString();
    }

    private static String toString(KeyFilter filter) {
        return "time " + Op.op2Str(filter.getOp()) + " " + filter.getValue();
    }

    private static String toString(ValueFilter filter) {
        if (filter.getOp().equals(Op.LIKE)) {
            return filter.getPath() + " regexp '" + filter.getValue().getBinaryVAsString() + "'";
        }
        return filter.getPath() + " " + Op.op2Str(filter.getOp()) + " " + filter.getValue().getValue();
    }

    private static String toString(OrFilter filter) {
        return filter.getChildren().stream().map(FilterTransformer::toString).collect(Collectors.joining(" or ", "(", ")"));
    }
}

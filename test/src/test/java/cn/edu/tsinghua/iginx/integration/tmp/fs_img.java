package cn.edu.tsinghua.iginx.integration.tmp;

import net.coobird.thumbnailator.Thumbnails;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;

public class fs_img {
    @Test
    public void img() throws Exception {
        Thumbnails.of(new File("D:\\Desktop\\Study\\My\\2021.5.1502872.JPG"))
            .scale(1f) //图片大小（长宽）压缩比例 从0-1，1表示原图
            .outputQuality(0.5f) //图片质量压缩比例 从0-1，越接近1质量越好
            .toOutputStream(new FileOutputStream("D:\\Desktop\\Study\\My\\test.jpg"));
    }
}


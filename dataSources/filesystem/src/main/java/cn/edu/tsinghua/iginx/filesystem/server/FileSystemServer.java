package cn.edu.tsinghua.iginx.filesystem.server;

import cn.edu.tsinghua.iginx.conf.Config;
import cn.edu.tsinghua.iginx.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileSystemServer implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(FileSystemServer.class);

    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    private final int port;

    private final Executor executor;


    @Override
    public void run() {

    }
}

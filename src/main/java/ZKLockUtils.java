import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.utils.CloseableUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Created by 王杰 on 2017/3/6.
 */
public class ZKLockUtils extends CuratorUtils {

    private static final Logger logger = LoggerFactory.getLogger(ZKLockUtils.class);

    private final int CONNECT_TIMEOUT = 10000;
    private final int RETRY_TIME = Integer.MAX_VALUE;
    private final int RETRY_INTERVAL = 1000;

    private String lockPath = "/dccs/tip/rests/lock";
    private String connectionString = "127.0.0.1";
    private CuratorFramework client = null;

    private InterProcessMutex lock = null;

    public ZKLockUtils() {
        super();
    }

    public void setPath(String path) {
        lockPath = path;
    }

    public void start() {

        try {
            client = super.newCurator(connectionString);

            client.start();

            lock = new InterProcessMutex(client, lockPath);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        try {
            CloseableUtils.closeQuietly(client);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public synchronized boolean acquireLock() throws Exception {
        if (!lock.acquire(10, TimeUnit.SECONDS)) {
            return false;
        }

        return true;
    }

    public synchronized void releaselock() throws Exception {
        lock.release();
    }

    @Test
    public void test() throws Exception {
        ZKLockUtils zkLockUtils = new ZKLockUtils();
        zkLockUtils.start();

        zkLockUtils.acquireLock();
        zkLockUtils.releaselock();
    }
}

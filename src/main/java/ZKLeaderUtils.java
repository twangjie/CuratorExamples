
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.utils.CloseableUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by 王杰 on 2017/3/6.
 */
public class ZKLeaderUtils extends CuratorUtils {

    private static final Logger logger = LoggerFactory.getLogger(ZKLeaderUtils.class);

    private String leaderPath = "/dccs/tip/rests/leader";
    private String connectionString = "127.0.0.1";
    private String strHostName = "";

    private CuratorFramework client = null;
    private LeaderLatch leaderLatch = null;

    private AtomicBoolean isLeader = new AtomicBoolean(false);

    public ZKLeaderUtils() {

        super();
    }

    public void setPath(String path) {
        leaderPath = path;
    }

    public void start() {

        try {
            client = super.newCurator(connectionString);
            client.start();

            leaderLatch = new LeaderLatch(client, leaderPath, "Client " + strHostName);
            leaderLatch.addListener(new LeaderLatchListener() {
                public void isLeader() {
                    isLeader.set(true);

                    logger.info("I'm leader ");

                }

                public void notLeader() {
                    isLeader.set(false);
                    logger.info("I'm not leader ");
                }
            });
            leaderLatch.start();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void stop() {
        try {
            CloseableUtils.closeQuietly(leaderLatch);
            CloseableUtils.closeQuietly(client);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public boolean isLeader() {
        return isLeader.get();
    }

    @Test
    public void test() throws Exception {
        ZKLeaderUtils zkLeaderUtils = new ZKLeaderUtils();
        zkLeaderUtils.start();

        while (true) {
            System.out.println("Is leader: " + zkLeaderUtils.isLeader());
            Thread.sleep(3000);
        }
    }
}

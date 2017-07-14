package leader;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.UUID;

/**
 * Created by 王杰 on 2017/7/14.
 */
public class LeaderLatchExample2 {

    private static final Logger logger = LoggerFactory.getLogger(LeaderLatchExample2.class);

    private static final String PATH = "/examples/leader";
    private static String connectingString = "127.0.0.1:2181";
    private final int CONNECT_TIMEOUT = 10000;
    private final int RETRY_TIME = Integer.MAX_VALUE;
    private final int RETRY_INTERVAL = 1000;
    private CuratorFramework client = null;
    private LeaderLatch leaderLatch = null;

    public static void main(String[] args) throws Exception {

        if (args.length == 1) {
            connectingString = args[0];
        }

        LeaderLatchExample2 leaderLatchExample2 = new LeaderLatchExample2();
        leaderLatchExample2.start();

        System.out.println("Press enter/return to quit");
        new BufferedReader(new InputStreamReader(System.in)).readLine();

        leaderLatchExample2.stop();
    }

    private CuratorFramework newCurator(String zkServers) {
        return CuratorFrameworkFactory.builder().connectString(zkServers)
                .retryPolicy(new RetryNTimes(RETRY_TIME, RETRY_INTERVAL))
                .connectionTimeoutMs(CONNECT_TIMEOUT).build();
    }

    public void start() {

        try {
            client = newCurator(connectingString);

            client.getCuratorListenable().addListener(new CuratorListener() {
                public void eventReceived(CuratorFramework client, CuratorEvent curatorEvent) throws Exception {
                    System.out.println(String.format("Received event %s ", curatorEvent.toString()));
                }
            });
            client.getConnectionStateListenable().addListener(new ConnectionStateListener() {

                public void stateChanged(CuratorFramework client, ConnectionState state) {

                    logger.info(String.format("Received state %s ", state.toString()));
//                    if (connectionState == ConnectionState.LOST) {
//                        while (true) {
//                            try {
//
//                                if (client.getZookeeperClient().blockUntilConnectedOrTimedOut()) {
//                                    System.out.println("reconnect to zk");
//                                    break;
//                                }
//                                else {
//                                    System.out.println("timeout");
//                                }
//                            } catch (InterruptedException e) {
//                                break;
//                            } catch (Exception e) {
//                                e.printStackTrace();
//                            }
//                        }
//                    }

                    if (state == ConnectionState.LOST) {
                        //连接丢失
                        logger.info("lost session with zookeeper");

//                        while (true) {
//                            try {
//                                if (client.getZookeeperClient().blockUntilConnectedOrTimedOut()) {
//                                    client.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL_SEQUENTIAL).forPath(zkRegPathPrefix, regContent.getBytes("UTF-8"));
//                                    break;
//                                }
//                            } catch (InterruptedException e) {
//                                break;
//                            } catch (Exception e) {
//                            }
//                        }


                    } else if (state == ConnectionState.CONNECTED) {
                        //连接新建
                        logger.info("connected with zookeeper");
                    } else if (state == ConnectionState.RECONNECTED) {
                        logger.info("reconnected with zookeeper");
                        //连接重连
//                        for(ZkStateListener s:stateListeners){
//                            s.reconnected();
//                        }
                    }
                }
            });

            client.start();

            leaderLatch = new LeaderLatch(client, PATH, "Client " + UUID.randomUUID().toString());
            leaderLatch.addListener(new LeaderLatchListener() {
                public void isLeader() {
                    System.out.println("I'm leader ");
                }

                public void notLeader() {
                    System.out.println("I'm not leader ");
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
}

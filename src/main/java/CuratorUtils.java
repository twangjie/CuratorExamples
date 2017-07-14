import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.framework.state.ConnectionStateListener;
import org.apache.curator.retry.RetryNTimes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

/**
 * Created by 王杰 on 2017/7/14.
 */
public class CuratorUtils implements CuratorListener, ConnectionStateListener {

    private static final Logger logger = LoggerFactory.getLogger(CuratorUtils.class);

    private final int CONNECT_TIMEOUT = 10000;
    private final int RETRY_TIME = Integer.MAX_VALUE;
    private final int RETRY_INTERVAL = 1000;

    private String strHostName = "";

    public CuratorUtils() {
        try {
            InetAddress addr = InetAddress.getLocalHost();
            strHostName = addr.getHostName();
            if (strHostName.isEmpty()) {
                strHostName = String.valueOf(addr.getHostAddress().hashCode());
            } else if (strHostName.indexOf('.') > 0) {
                strHostName = strHostName.substring(0, strHostName.indexOf('.'));
            }

        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
    }

    public CuratorFramework newCurator(String connectingString ) {

        CuratorFramework client = CuratorFrameworkFactory.builder().connectString(connectingString)
                .retryPolicy(new RetryNTimes(RETRY_TIME, RETRY_INTERVAL))
                .connectionTimeoutMs(CONNECT_TIMEOUT).build();

        client.getCuratorListenable().addListener(this);
        client.getConnectionStateListenable().addListener(this);

        return client;
    }

    public static boolean exists(CuratorFramework client, String path) throws Exception {
        Stat stat = client.checkExists().forPath(path);
        return stat != null;
    }


    public static void create(CuratorFramework client, String path,
                              byte[] payload) throws Exception {
        // this will create the given ZNode with the given data
        client.create().creatingParentsIfNeeded().forPath(path, payload);
    }

    public static void createEphemeral(CuratorFramework client, String path,
                                       byte[] payload) throws Exception {
        // this will create the given EPHEMERAL ZNode with the given data
        client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL).forPath(path, payload);
    }

    public static String createEphemeralSequential(CuratorFramework client,
                                                   String path, byte[] payload) throws Exception {
        // this will create the given EPHEMERAL-SEQUENTIAL ZNode with the given
        // data using Curator protection.
        return client.create().creatingParentsIfNeeded().withProtection()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(path, payload);
    }

    public static void setData(CuratorFramework client, String path,
                        byte[] payload) throws Exception {
        // set data for the given node
        client.setData().forPath(path, payload);
    }

    public static byte[] getData(CuratorFramework client, String path) throws Exception {
        return client.getData().forPath(path);
    }

    public static void setDataAsync(CuratorFramework client, String path,
                             byte[] payload) throws Exception {
        // this is one method of getting event/async notifications
        CuratorListener listener = new CuratorListener() {
            @Override
            public void eventReceived(CuratorFramework client,
                                      CuratorEvent event) throws Exception {
                System.out.println("setDataAsync: " + event);
            }
        };
        client.getCuratorListenable().addListener(listener);
        // set data for the given node asynchronously. The completion
        // notification
        // is done via the CuratorListener.
        client.setData().inBackground().forPath(path, payload);
    }

    public static void setDataAsyncWithCallback(CuratorFramework client,
                                         BackgroundCallback callback, String path, byte[] payload)
            throws Exception {
        // this is another method of getting notification of an async completion
        client.setData().inBackground(callback).forPath(path, payload);
    }

    public static void delete(CuratorFramework client, String path)
            throws Exception {
        // delete the given node
        client.delete().deletingChildrenIfNeeded().forPath(path);
    }

    public static void guaranteedDelete(CuratorFramework client, String path)
            throws Exception {
        // delete the given node and guarantee that it completes
        client.delete().guaranteed().forPath(path);
    }

    public static List<String> watchedGetChildren(CuratorFramework client,
                                           String path) throws Exception {
        /**
         * Get children and set a watcher on the node. The watcher notification
         * will come through the CuratorListener (see setDataAsync() above).
         */
        return client.getChildren().watched().forPath(path);
    }

    public static List<String> watchedGetChildren(CuratorFramework client,
                                           String path, Watcher watcher) throws Exception {
        /**
         * Get children and set the given watcher on the node.
         */
        return client.getChildren().usingWatcher(watcher).forPath(path);
    }

    public void eventReceived(CuratorFramework client, CuratorEvent curatorEvent) throws Exception {
        System.out.println(String.format("Received event %s ", curatorEvent.toString()));
    }

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
}

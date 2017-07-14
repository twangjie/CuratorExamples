
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.test.TestingServer;
import org.junit.Test;

/**
 * Created by 王杰 on 2017/07/14.
 */
public class TestCase {

    @Test
    public void testSetData() throws Exception {

        TestingServer server = new TestingServer(2181);
        server.start();

        CuratorUtils curatorUtils = new CuratorUtils();
        CuratorFramework client = curatorUtils.newCurator(server.getConnectString());
        client.start();

        String path = "/test";
        if(!CuratorUtils.exists(client, path)) {
            CuratorUtils.create(client, path, null);
        }
        CuratorUtils.setData(client, path, "{\"name:\",\"test\"}".getBytes());
        byte bt[] = CuratorUtils.getData(client, path);
        if(bt != null){
            String ret = new String(bt);
            System.out.println(ret);
        }

        CuratorUtils.setData(client, path, "{\"name:\",\"test123\"}".getBytes());
        bt = CuratorUtils.getData(client, path);
        if(bt != null){
            String ret = new String(bt);
            System.out.println(ret);
        }
    }

}

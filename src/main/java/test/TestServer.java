package test;

import org.apache.curator.test.TestingServer;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * Created by 王杰 on 2017/7/14.
 */
public class TestServer {

    public static void main(String args[]) throws Exception {
        TestingServer server = new TestingServer(2181);
        server.start();

        System.out.println("Press enter/return to quit\n");
        String ret = new BufferedReader(new InputStreamReader(System.in)).readLine();
        System.exit(0);
    }
}

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class ClusterHealer implements Watcher {

    // Path to the worker jar
    private static String pathToProgram = "target/cluster-healer-1.0-SNAPSHOT-jar-with-dependencies.jar";
    // The number of worker instances we need to maintain at all times
    private static int numberOfWorkers = 4;

    public static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    public static final int SESSION_TIMEOUT = 3000;
    private static final String WORKERS_PARENT_ZNODE = "/workers";
    private static final String WORKER_ZNODE = "/worker_";
    private ZooKeeper zooKeeper;

    public ClusterHealer(int numberOfWorkers, String pathToProgram) {
        this.numberOfWorkers = numberOfWorkers;
        this.pathToProgram = pathToProgram;
    }

    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ClusterHealer clusterHealer = new ClusterHealer(numberOfWorkers, pathToProgram);
        clusterHealer.connectToZookeeper();
        for (int i = 0; i < numberOfWorkers; i++) {
            clusterHealer.initialiseCluster();
        }
        clusterHealer.checkRunningWorkers();
        clusterHealer.run();

        System.out.println("Disconnected from Zookeeper, exiting application");
        clusterHealer.close();
    }

    /**
     * Check if the `/workers` parent znode exists, and create it if it doesn't. Decide for yourself what type of znode
     * it should be (e.g.persistent, ephemeral etc.). Check if workers need to be launched.
     */
    public void initialiseCluster() throws KeeperException, InterruptedException {
        String znodeFullPath = zooKeeper.create(WORKERS_PARENT_ZNODE + WORKER_ZNODE, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("znode name " + znodeFullPath);
    }

    /**
     * Instantiates a Zookeeper client, creating a connection to the Zookeeper server.
     */
    public void connectToZookeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    /**
     * Keeps the application running waiting for Zookeeper events.
     */
    public void run() throws InterruptedException {
        synchronized (zooKeeper) {
            zooKeeper.wait();
        }
    }

    /**
     * Closes the Zookeeper client connection.
     */
    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    /**
     * Handles Zookeeper events related to: - Connecting and disconnecting from the Zookeeper server. - Changes in the
     * number of workers currently running.
     *
     * @param event A Zookeeper event
     */
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case None:
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Successfully connected to ZooKeeper.");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected from ZooKeeper.");
                        zooKeeper.notifyAll();
                    }
                }
            case NodeCreated:
                System.out.println("Node created.");
                checkRunningWorkers();
            case NodeDeleted:
                try {
                    System.out.println("Received node deletion event.");
                    initialiseCluster();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
            case NodeChildrenChanged:
                try {
                    if(zooKeeper.getAllChildrenNumber(WORKERS_PARENT_ZNODE) < numberOfWorkers) {
                        startWorker();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (KeeperException e) {
                    e.printStackTrace();
                }
        }
    }

    /**
     * Checks how many workers are currently running.
     * If less than the required number, then start a new worker.
     */
    public void checkRunningWorkers() {
        try {
            System.out.println(zooKeeper.getAllChildrenNumber(WORKERS_PARENT_ZNODE) + " workers are running.");
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    /**
     * Starts a new worker using the path provided as a command line parameter.
     *
     * @throws IOException
     */
    public void startWorker() throws IOException {
        File file = new File(pathToProgram);
        String command = "java -jar " + file.getName();

        System.out.println(String.format("Launching worker instance : %s ", command));
        Runtime.getRuntime().exec(command, null, file.getParentFile());
    }

}

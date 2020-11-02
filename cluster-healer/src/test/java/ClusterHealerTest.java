import com.github.blindpirate.extensions.CaptureSystemOutput;
import org.apache.curator.test.TestingServer;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.jupiter.api.*;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsStringIgnoringCase;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;

class ClusterHealerTest {

    private static final String WORKER_ZNODE = "/worker_";
    private static final String PATH_TO_PROGRAM = "./";
    private static TestingServer zkServer;
    private static ZooKeeper zooKeeper;
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final int SESSION_TIMEOUT = 3000;
    private static final String WORKERS_PARENT_ZNODE = "/workers";


    @BeforeAll
    public static void setUp() throws Exception {
        zkServer = new TestingServer(2181, true);
        zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, watchedEvent -> {
        });
    }

    @AfterEach
    private void tearDown() throws KeeperException, InterruptedException {
        if (zooKeeper.exists(WORKERS_PARENT_ZNODE, false) != null) {
            ZKUtil.deleteRecursive(zooKeeper, WORKERS_PARENT_ZNODE);
        }
    }

    @AfterAll
    public static void shutDown() throws Exception {
        zkServer.stop();
    }

    // Check if ClusterHealer connects to zookeeper and prints out a connected message
    @Test
    @CaptureSystemOutput
    public void processConnected(CaptureSystemOutput.OutputCapture outputCapture) throws IOException, InterruptedException {
        ClusterHealer healer = new ClusterHealer(0, PATH_TO_PROGRAM);
        outputCapture.expect(containsStringIgnoringCase("connected"));
        healer.connectToZookeeper();
        healer.close();
    }

    // Check if ClusterHealer disconnects from zookeeper and prints out a disconnected message
    @Test
    @CaptureSystemOutput
    public void processDisconnected(CaptureSystemOutput.OutputCapture outputCapture) throws IOException, InterruptedException {
        ClusterHealer healer = new ClusterHealer(0, PATH_TO_PROGRAM);
        healer.connectToZookeeper();
        outputCapture.expect(containsStringIgnoringCase("disconnected"));
        healer.close();
    }

    // Check if /workers znode exists. If it doesn't, create it.
    @Test
    void createsWorkersParentZnodeDoesntExist() throws KeeperException, InterruptedException, IOException {
        ClusterHealer healer = new ClusterHealer(0, PATH_TO_PROGRAM);
        healer.connectToZookeeper();
        healer.initialiseCluster();
        Stat result = zooKeeper.exists(WORKERS_PARENT_ZNODE, false);
        assertNotNull(result);
        healer.close();
    }

    // Check if /workers znode exists. If it does, don't try to create it.
    @Test
    void createsWorkersParentZnodeAlreadyExists() throws KeeperException, InterruptedException, IOException {
        helperCreateZnode(WORKERS_PARENT_ZNODE, CreateMode.PERSISTENT);
        ClusterHealer healer = new ClusterHealer(0, PATH_TO_PROGRAM);
        healer.connectToZookeeper();
        healer.initialiseCluster();
        healer.close();
    }

    // Gets current number of child znodes
    // Check that startNewWorker is called if number is below required
    @Test
    void launchWorkersIfNecessary() throws IOException, InterruptedException, KeeperException {
        ClusterHealer spyHealer = Mockito.spy(new ClusterHealer(3, PATH_TO_PROGRAM));
        doNothing().when(spyHealer).startWorker();

        helperCreateWorkerParentZnode();
        helperCreateWorkerChildZnodes(2);

        spyHealer.connectToZookeeper();
        spyHealer.checkRunningWorkers();

        verify(spyHealer).startWorker();
        spyHealer.close();
    }

    // Gets current number of child znodes
    // Check that startNewWorker is not called is number is equal to required
    @Test
    void dontLaunchWorkersIfNotNecessary() throws IOException, InterruptedException, KeeperException {
        ClusterHealer spyHealer = Mockito.spy(new ClusterHealer(2, PATH_TO_PROGRAM));

        helperCreateWorkerParentZnode();
        helperCreateWorkerChildZnodes(2);
        spyHealer.connectToZookeeper();
        spyHealer.checkRunningWorkers();

        verify(spyHealer, never()).startWorker();
        spyHealer.close();
    }

    // Make sure launchWorkersIfNecessary() is called if event is correct
    @Test
    void processCorrectEvent() {
        ClusterHealer spyHealer = Mockito.spy(new ClusterHealer(3, PATH_TO_PROGRAM));
        ClusterHealer mockHealer = mock(ClusterHealer.class);
        doCallRealMethod().when(mockHealer).process(any(WatchedEvent.class));
        mockHealer.process(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, WORKERS_PARENT_ZNODE));

        verify(mockHealer).checkRunningWorkers();
    }

    // Make sure launchWorkersIfNecessary() isn't called if event is not correct
    @Test
    void dontProcessInCorrectEvents() {
        ClusterHealer spyHealer = Mockito.spy(new ClusterHealer(3, PATH_TO_PROGRAM));
        ClusterHealer mockHealer = mock(ClusterHealer.class);
        doCallRealMethod().when(mockHealer).process(any(WatchedEvent.class));
        for (WatchedEvent event : getEvents()) {
            mockHealer.process(event);
        }
        verify(mockHealer, never()).checkRunningWorkers();
    }

    void helperCreateWorkerParentZnode() throws KeeperException, InterruptedException {
        helperCreateZnode(WORKERS_PARENT_ZNODE, CreateMode.PERSISTENT);
    }

    void helperCreateWorkerChildZnodes(int numberOfWorkerChildren) throws KeeperException, InterruptedException {
        for (int i = 0; i < numberOfWorkerChildren; i++) {
            helperCreateZnode(WORKERS_PARENT_ZNODE + WORKER_ZNODE, CreateMode.EPHEMERAL_SEQUENTIAL);
        }
    }

    void helperCreateZnode(String path, CreateMode mode) throws KeeperException, InterruptedException {
        zooKeeper.create(path, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, mode);
    }

    List<WatchedEvent> getEvents() {
        return Arrays.asList(
                new WatchedEvent(Watcher.Event.EventType.ChildWatchRemoved, Watcher.Event.KeeperState.SyncConnected, WORKERS_PARENT_ZNODE),
                new WatchedEvent(Watcher.Event.EventType.PersistentWatchRemoved, Watcher.Event.KeeperState.SyncConnected, WORKERS_PARENT_ZNODE),
                new WatchedEvent(Watcher.Event.EventType.NodeCreated, Watcher.Event.KeeperState.SyncConnected, WORKERS_PARENT_ZNODE),
                new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, WORKERS_PARENT_ZNODE),
                new WatchedEvent(Watcher.Event.EventType.NodeDeleted, Watcher.Event.KeeperState.SyncConnected, WORKERS_PARENT_ZNODE),
                new WatchedEvent(Watcher.Event.EventType.DataWatchRemoved, Watcher.Event.KeeperState.SyncConnected, WORKERS_PARENT_ZNODE),
                new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.SyncConnected, WORKERS_PARENT_ZNODE)
        );
    }
}
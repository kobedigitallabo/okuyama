package test.helper.manager;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import test.helper.manager.DataNode.CompressionMode;
import test.helper.manager.DataNode.StorageMode;

/**
 * OkuyamaHelperのテスト。
 * @author s-ito
 *
 */
public class OkuyamaHelperTest {
	
	private OkuyamaHelper helper = new OkuyamaHelper();
	
	@Before
	public void setUp() throws Exception {
	}

	@After
	public void tearDown() throws Exception {
	}

	@Test
	public void serverrun() throws Exception {
		// okuyama起動
		OkuyamaManager manager = this.helper.getServerrun();
		try {
			manager.bootOkuyama(System.out, System.err);
			// setとgetによる疎通確認
			OkuyamaClient client = new OkuyamaClient();
			client.connect("localhost", 8888);
			client.setValue("test", "test2342w54235353533435353535234242321523542");
			String[] result = client.getValue("test");
			assertEquals(result[0], "true");
			assertEquals(result[1], "test2342w54235353533435353535234242321523542");
			client.close();
			// Nodeの情報をしっかり管理できているか確認
			OkuyamaMachine machine = manager.getOkuyamaMachine("localhost");
			OkuyamaProcess dataNodeProcess = machine.getNodeProcess("DataNode");
			OkuyamaNode[] dataNodes = dataNodeProcess.getNodes();
			assertEquals(dataNodes.length, 2);
			for (int i = 0;i < 2;i++) {
				assertEquals(dataNodes[i].getNodeHostName(), "127.0.0.1");
				assertEquals(dataNodes[i].getManagementPort(), 15553);
				assertTrue(dataNodes[i] instanceof DataNode);
				DataNode node = (DataNode) dataNodes[i];
				String jobName = dataNodes[i].getNodeJobName();
				switch (jobName) {
				case "KeyManagerJob1":
					assertEquals(dataNodes[i].getNodePort(), 5553);
					assertEquals(node.getAnticipatedSize(), 100000);
					assertNull(node.getCache());
					assertEquals(node.getCompresstionMode(), CompressionMode.LOW);
					assertEquals(node.getJournalFile(), "./keymapfile/1.work.key");
					assertNull(node.getKeyDirectory());
					assertEquals(node.getMemoryLimit(), 80);
					assertNull(node.getSerializeClassName());
					assertEquals(node.getShareDataFileMaxDelayCount(), 0);
					assertEquals(node.getSnapshotFile(), "./keymapfile/1.key.obj");
					assertEquals(node.getStorageMode(), StorageMode.MEMORY);
					assertEquals(node.getValueLimitSize(), -1);
					assertArrayEquals(node.getVirtualMemory(), new String[]{"./keymapfile/virtualdata1/"});
					assertFalse(node.isSerialize());
					assertTrue(node.isDataSaveTransactionFileEveryCommit());
					break;
				case "KeyManagerJob2":
					assertEquals(dataNodes[i].getNodePort(), 5554);
					assertEquals(node.getAnticipatedSize(), 100000);
					assertNull(node.getCache());
					assertEquals(node.getCompresstionMode(), CompressionMode.LOW);
					assertEquals(node.getJournalFile(), "./keymapfile/2.work.key");
					assertNull(node.getKeyDirectory());
					assertEquals(node.getMemoryLimit(), 80);
					assertNull(node.getSerializeClassName());
					assertEquals(node.getShareDataFileMaxDelayCount(), 0);
					assertEquals(node.getSnapshotFile(), "./keymapfile/2.key.obj");
					assertEquals(node.getStorageMode(), StorageMode.MEMORY);
					assertEquals(node.getValueLimitSize(), -1);
					assertArrayEquals(node.getVirtualMemory(), new String[]{"./keymapfile/virtualdata2/"});
					assertFalse(node.isSerialize());
					assertTrue(node.isDataSaveTransactionFileEveryCommit());
					break;
				default:
					fail();
				}
			}
			OkuyamaProcess masterNodeProcess = machine.getNodeProcess("MasterNode");
			OkuyamaNode[] masterNodes = masterNodeProcess.getNodes();
			assertEquals(masterNodes.length, 1);
			assertEquals(masterNodes[0].getManagementPort(), 18888);
			assertEquals(masterNodes[0].getNodeHostName(), "127.0.0.1");
			assertEquals(masterNodes[0].getNodeJobName(), "MasterManagerJob");
			assertEquals(masterNodes[0].getNodePort(), 8888);
			assertTrue(((MasterNode)masterNodes[0]).isMain());
		} catch (Exception e) {
			throw e;
		} finally {
			// 停止させる
			assertTrue(manager.shutdownOkuyama());
			this.helper.cleanOkuyama(manager);
		}
	}

}

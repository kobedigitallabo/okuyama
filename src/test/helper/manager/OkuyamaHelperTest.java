package test.helper.manager;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

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
				String jobName = dataNodes[i].getNodeJobName();
				switch (jobName) {
				case "KeyManagerJob1":
					assertEquals(dataNodes[i].getNodePort(), 5553);
					break;
				case "KeyManagerJob2":
					assertEquals(dataNodes[i].getNodePort(), 5554);
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

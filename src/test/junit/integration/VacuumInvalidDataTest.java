package test.junit.integration;

import static org.junit.Assert.*;

import java.nio.charset.Charset;
import java.util.Properties;

import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import test.helper.manager.OkuyamaHelper;
import test.helper.manager.OkuyamaMachine;
import test.helper.manager.OkuyamaManager;
import test.helper.manager.OkuyamaNode;
import test.helper.manager.OkuyamaProcess;
import test.helper.manager.OkuyamaUtil;

import com.sun.mail.util.BASE64EncoderStream;

/**
 * 有効期限が過ぎたデータの削除チェック(VacuumInvalidData)する際のArrayIndexOutOfBoundsException発生検証・テスト用クラス。
 * @author s-ito
 *
 */
public class VacuumInvalidDataTest {
	
	private OkuyamaHelper okuyamaHelper = new OkuyamaHelper();
	
	private OkuyamaManager okuyamaManager;
	
	private OkuyamaClient okuyamaClient;
	
	private String host;
	
	private int port;
	
	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		this.okuyamaManager = this.okuyamaHelper.getServerrun();
		Properties prop = this.okuyamaManager.getManagementInfo();
		prop.setProperty("OkuyamaProcess.DataNode.okuyamaOptionCount", "4");
		prop.setProperty("OkuyamaProcess.DataNode.okuyamaOption.4", "-svic 1");
		prop.setProperty("OkuyamaProcess.DataNode.okuyamaOption.2", "-vidf true");
		this.okuyamaHelper.cleanOkuyama(this.okuyamaManager);
		this.okuyamaManager.bootOkuyama(System.out, System.err);
		OkuyamaMachine machine = this.okuyamaManager.getOkuyamaMachine("localhost");
		OkuyamaProcess process = machine.getNodeProcess("MasterNode");
		OkuyamaNode[] masterNodes = process.getNodes();
		this.host = masterNodes[0].getNodeHostName();
		this.port = masterNodes[0].getNodePort();
		this.okuyamaClient = new OkuyamaClient();
		this.okuyamaClient.connect(host, port);
	}

	@After
	public void tearDown() throws Exception {
		this.okuyamaClient.close();
		this.okuyamaManager.shutdownOkuyama();
		this.okuyamaHelper.cleanOkuyama(this.okuyamaManager);
	}

	@Test
	public void test() throws Exception {
		String key = new String(BASE64EncoderStream.encode("NEWKEY1".getBytes(Charset.defaultCharset().name())));
		String value = new String(BASE64EncoderStream.encode("TEXT_DATA".getBytes(Charset.defaultCharset().name())));
		OkuyamaUtil.connectByTelnet(this.host, this.port, "6," + key + ",(B),0," + value + ",3");
		// 有効期限延長
		Thread.sleep(1500);
		String [] result = null;
		try {
			result = this.okuyamaClient.getValueAndUpdateExpireTime("NEWKEY1");
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertNull(result);
		// 有効期限内に取得
		Thread.sleep(1500);
		try {
			result = this.okuyamaClient.getValue("NEWKEY1");
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertNull(result);
		// 有効期限切れ後取得して失敗
		Thread.sleep(3000);
		try {
			result = this.okuyamaClient.getValue("NEWKEY1");
		} catch (Exception e) {
			e.printStackTrace();
		}
		assertNull(result);
		// 一定時間待ってみる
		for (int i = 1;i <= 80;i++) {
			Thread.sleep(3000);
			if ((i % 5) == 0) {
				assertTrue(this.okuyamaHelper.pingOkuyama(this.okuyamaManager));
			}
		}
		assertTrue(this.okuyamaHelper.pingOkuyama(this.okuyamaManager));
	}

}

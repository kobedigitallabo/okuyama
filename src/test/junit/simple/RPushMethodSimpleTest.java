package test.junit.simple;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * listRPushメソッドの簡単なテスト。
 * @author s-ito
 *
 */
public class RPushMethodSimpleTest {
	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;
	
	private String listData;

	@Before
	public void setUp() throws Exception {
		RPushMethodSimpleTest.helper.init();
		RPushMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient = RPushMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// List構造準備
		this.listName = RPushMethodSimpleTest.helper.createTestDataKey(false, 0);
		this.listData = RPushMethodSimpleTest.helper.createTestDataKey(false, 1);
		String[] ret = this.okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
	}

	@After
	public void tearDown() throws Exception {
		RPushMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void リストの末尾にデータを追加する() throws Exception {
		String[] ret = okuyamaClient.listRPush(this.listName, this.listData);
		assertEquals(ret[0], "true");
	}
	
	@Test
	public void 存在しないリストの末尾にデータを追加しようとして失敗する() throws Exception {
		String[] ret = okuyamaClient.listRPush(this.listName + "_nothing", this.listData);
		assertEquals(ret[0], "false");
	}
	
	@Test
	public void 同じデータをリストの末尾に追加する() throws Exception {
		String[] ret = okuyamaClient.listRPush(this.listName, this.listData);
		assertEquals(ret[0], "true");
		ret = okuyamaClient.listRPush(this.listName, this.listData);
		assertEquals(ret[0], "true");
	}
}

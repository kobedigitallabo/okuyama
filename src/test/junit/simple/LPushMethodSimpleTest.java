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
 * listLPushメソッドの簡単なテスト。
 * @author s-ito
 *
 */
public class LPushMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;
	
	private String listData;

	@Before
	public void setUp() throws Exception {
		LPushMethodSimpleTest.helper.init();
		LPushMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient = LPushMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// List構造準備
		this.listName = LPushMethodSimpleTest.helper.createTestDataKey(false, 0);
		this.listData = LPushMethodSimpleTest.helper.createTestDataKey(false, 1);
		String[] ret = this.okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
	}

	@After
	public void tearDown() throws Exception {
		LPushMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void リストの先頭にデータを追加する() throws Exception {
		String[] ret = okuyamaClient.listLPush(this.listName, this.listData);
		assertEquals(ret[0], "true");
	}
	
	@Test
	public void 存在しないリストの先頭にデータを追加しようとして失敗する() throws Exception {
		String[] ret = okuyamaClient.listLPush(this.listName + "_nothing", this.listData);
		assertEquals(ret[0], "false");
	}
	
	@Test
	public void 同じデータをリストの先頭に追加する() throws Exception {
		String[] ret = okuyamaClient.listLPush(this.listName, this.listData);
		assertEquals(ret[0], "true");
		ret = okuyamaClient.listLPush(this.listName, this.listData);
		assertEquals(ret[0], "true");
	}
}

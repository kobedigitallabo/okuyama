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
 * listLPopの簡単なテスト。
 * @author s-ito
 *
 */
public class LPopMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;
	
	private String listData;

	@Before
	public void setUp() throws Exception {
		LPopMethodSimpleTest.helper.init();
		LPopMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient = LPopMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// List構造準備
		this.listName = LPopMethodSimpleTest.helper.createTestDataKey(false, 0);
		this.listData = LPopMethodSimpleTest.helper.createTestDataKey(false, 1);
		String[] ret = this.okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
		ret = this.okuyamaClient.listLPush(this.listName, this.listData);
		assertEquals(ret[0], "true");
	}

	@After
	public void tearDown() throws Exception {
		LPopMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void リストの先頭からデータを取り出す() throws Exception {
		String[] ret = okuyamaClient.listLPop(this.listName);
		assertEquals(ret[0], "true");
		assertEquals(ret[1], this.listData);
	}
	
	@Test
	public void 存在しないリストの先頭からデータを取り出そうとして失敗する() throws Exception {
		String[] ret = okuyamaClient.listLPop(this.listName + "_nothing");
		assertEquals(ret[0], "false");
	}
	
	@Test
	public void データが無いリストからデータを取り出そうとして失敗する() throws Exception {
		String[] ret = okuyamaClient.listLPop(this.listName);
		assertEquals(ret[0], "true");
		assertEquals(ret[1], this.listData);
		ret = okuyamaClient.listLPop(this.listName);
		assertEquals(ret[0], "false");
	}
}

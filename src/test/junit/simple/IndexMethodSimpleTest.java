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
 * listIndexの簡単なテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class IndexMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;
	
	private String listData;

	@Before
	public void setUp() throws Exception {
		IndexMethodSimpleTest.helper.init();
		IndexMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient = IndexMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// List構造準備
		this.listName = IndexMethodSimpleTest.helper.createTestDataKey(false, 0);
		this.listData = IndexMethodSimpleTest.helper.createTestDataKey(false, 1);
		String[] ret = this.okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
		ret = this.okuyamaClient.listLPush(this.listName, this.listData);
		assertEquals(ret[0], "true");
	}

	@After
	public void tearDown() throws Exception {
		IndexMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void リストからデータを取得する() throws Exception {
		String[] ret = okuyamaClient.listIndex(this.listName, 0);
		assertEquals(ret[0], "true");
		assertEquals(ret[1], this.listData);
	}
	
	@Test
	public void 存在しないリストからデータを取得しようとして失敗する() throws Exception {
		String[] ret = okuyamaClient.listIndex(this.listName + "_nothing", 0);
		assertEquals(ret[0], "false");
	}
	
	@Test
	public void 存在しないデータをリストから取得しようとして失敗する() throws Exception {
		String[] ret = okuyamaClient.listIndex(this.listName, 1);
		assertEquals(ret[0], "false");
	}

}

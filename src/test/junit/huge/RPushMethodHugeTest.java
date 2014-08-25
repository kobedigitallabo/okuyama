package test.junit.huge;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * 巨大データに対するlistRPushメソッドのテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class RPushMethodHugeTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;
	
	private String listData;

	@Before
	public void setUp() throws Exception {
		RPushMethodHugeTest.helper.init();
		RPushMethodHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient = RPushMethodHugeTest.helper.getConnectedOkuyamaClient();
		// List構造準備
		this.listName = RPushMethodHugeTest.helper.createTestDataKey(false, 0);
		this.listData = RPushMethodHugeTest.helper.createTestDataKey(true, 1);
		String[] ret = this.okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
	}

	@After
	public void tearDown() throws Exception {
		RPushMethodHugeTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void リストの末尾に巨大データを追加する() throws Exception {
		String[] ret = okuyamaClient.listRPush(this.listName, this.listData);
		assertEquals(ret[0], "true");
	}

}

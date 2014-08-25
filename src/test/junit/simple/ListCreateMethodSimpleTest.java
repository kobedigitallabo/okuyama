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
 * List構造作成テストです。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class ListCreateMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;
	
	private String listName;

	@Before
	public void setUp() throws Exception {
		ListCreateMethodSimpleTest.helper.init();
		ListCreateMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  ListCreateMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// List構造名作成
		this.listName = ListCreateMethodSimpleTest.helper.createTestDataKey(false);
	}

	@After
	public void tearDown() throws Exception {
		ListCreateMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void List構造を作成する() throws Exception {
		String[] ret = okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
	}
	
	@Test
	public void 既存List構造と同じ名前のList構造を作成使用として失敗する() throws Exception {
		String[] ret = okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "true");
		ret = okuyamaClient.createListStruct(this.listName);
		assertEquals(ret[0], "false");
	}
}

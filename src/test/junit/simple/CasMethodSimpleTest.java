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
 * CAS操作の簡単なテスト
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class CasMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String testDataKey;

	private String testDataValue;

	@Before
	public void setUp() throws Exception {
		CasMethodSimpleTest.helper.init();
		CasMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  CasMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		this.testDataKey = CasMethodSimpleTest.helper.createTestDataKey(false);
		this.testDataValue = CasMethodSimpleTest.helper.createTestDataValue(false);
		this.okuyamaClient.setValue(this.testDataKey, this.testDataValue);
	}

	@After
	public void tearDown() throws Exception {
		CasMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}
	
	@Test
	public void 取得したデータのバージョン情報を使ってデータを更新する() throws Exception {
		Object[] getsRet = this.okuyamaClient.getValueVersionCheck(this.testDataKey);
		assertEquals(getsRet[0], "true");
		assertEquals(getsRet[1], this.testDataValue);
		String[] retParam = this.okuyamaClient.setValueVersionCheck(this.testDataKey,
																	this.testDataValue + "_Updated", (String)getsRet[2]);
		assertEquals(retParam[0], "true");
		String[] getRet = this.okuyamaClient.getValue(this.testDataKey);
		assertEquals(getRet[0], "true");
		assertEquals(getRet[1], this.testDataValue + "_Updated");
	}
	
	@Test
	public void 取得したデータのバージョン情報を使って2回データを更新使用として2回目で失敗する() throws Exception {
		Object[] getsRet = this.okuyamaClient.getValueVersionCheck(this.testDataKey);
		assertEquals(getsRet[0], "true");
		assertEquals(getsRet[1], this.testDataValue);
		String[] retParam = this.okuyamaClient.setValueVersionCheck(this.testDataKey,
																	this.testDataValue + "_Updated", (String)getsRet[2]);
		assertEquals(retParam[0], "true");
		retParam = this.okuyamaClient.setValueVersionCheck(this.testDataKey,
															this.testDataValue + "_Updated2", (String)getsRet[2]);
		assertEquals(retParam[0], "false");
		String[] getRet = this.okuyamaClient.getValue(this.testDataKey);
		assertEquals(getRet[0], "true");
		assertEquals(getRet[1], this.testDataValue + "_Updated");
	}

}

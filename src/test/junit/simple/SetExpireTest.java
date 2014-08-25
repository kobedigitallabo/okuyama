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
 * 有効期限付きデータの操作テスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class SetExpireTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String testDataKey;

	private String testDataValue;

	@Before
	public void setUp() throws Exception {
		SetExpireTest.helper.init();
		SetExpireTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetExpireTest.helper.getConnectedOkuyamaClient();
		this.testDataKey = SetExpireTest.helper.createTestDataKey(false);
		this.testDataValue = SetExpireTest.helper.createTestDataValue(false);
		this.okuyamaClient.setValue(this.testDataKey, this.testDataValue, Integer.valueOf(3));
	}

	@After
	public void tearDown() throws Exception {
		SetExpireTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void 有効期限内にデータを取得する() throws Exception {
		String[] result = this.okuyamaClient.getValue(this.testDataKey);
		assertEquals(result[0], "true");
		assertEquals(result[1], this.testDataValue);
	}

	@Test
	public void 有効期限切れのデータを取得しようとして失敗する() throws Exception {
		Thread.sleep(4000);
		String[] result = this.okuyamaClient.getValue(this.testDataKey);
		assertEquals(result[0], "false");
	}

	@Test
	public void 有効期限を延長してデータを取得する() throws Exception {
		Thread.sleep(2000);
		String[] result = this.okuyamaClient.getValueAndUpdateExpireTime(this.testDataKey);
		assertEquals(result[0], "true");
		assertEquals(result[1], this.testDataValue);
		Thread.sleep(2000);
		assertEquals(result[0], "true");
		assertEquals(result[1], this.testDataValue);
	}

}

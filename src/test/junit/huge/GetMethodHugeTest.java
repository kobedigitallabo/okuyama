package test.junit.huge;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import test.junit.MethodTestHelper;

/**
 * 巨大データに対するgetメソッドテスト。
 * @author s-ito
 *
 */
public class GetMethodHugeTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String testDataKey;

	private String testDataValue;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		GetMethodHugeTest.helper.init();
		GetMethodHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient =  GetMethodHugeTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		this.testDataKey = GetMethodHugeTest.helper.createTestDataKey(false);
		this.testDataValue = GetMethodHugeTest.helper.createTestDataValue(false);
		this.okuyamaClient.setValue(this.testDataKey, this.testDataValue);
	}

	@After
	public void tearDown() throws Exception {
		// テストデータを破棄
		try {
			this.okuyamaClient.removeValue(this.testDataKey);
		} catch (OkuyamaClientException e) {
		}

		this.okuyamaClient.close();
	}

	@Test
	public void キーに対応した値が取得する() throws Exception {
		String[] result = this.okuyamaClient.getValue(this.testDataKey);
		if (result[0].equals("true")) {
			assertEquals(result[1], this.testDataValue);
		} else {
			fail("getメソッドエラー");
		}
	}


}

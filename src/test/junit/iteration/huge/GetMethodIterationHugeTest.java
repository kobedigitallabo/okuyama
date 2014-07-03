package test.junit.iteration.huge;

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
 * 巨大データに対するgetメソッドの繰り返しテスト。
 * @author s-ito
 *
 */
public class GetMethodIterationHugeTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		GetMethodIterationHugeTest.helper.init();
		GetMethodIterationHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient =  GetMethodIterationHugeTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		for (int i = 0;i < 5000;i++) {
			String testDataKey = GetMethodIterationHugeTest.helper.createTestDataKey(true, i);
			String testDataValue = GetMethodIterationHugeTest.helper.createTestDataValue(true, i);
			this.okuyamaClient.setValue(testDataKey, testDataValue);
		}
	}

	@After
	public void tearDown() throws Exception {
		// テストデータを破棄
		try {
			for (int i = 0;i < 5000;i++) {
				String testDataKey = GetMethodIterationHugeTest.helper.createTestDataKey(true, i);
				this.okuyamaClient.removeValue(testDataKey);
			}
		} catch (OkuyamaClientException e) {
		}
		this.okuyamaClient.close();
	}

	@Test
	public void キーに対応した巨大な値を500回取得する() throws Exception {
		for (int i = 0;i < 500;i++) {
			String testDataKey = GetMethodIterationHugeTest.helper.createTestDataKey(true, i);
			String testDataValue = GetMethodIterationHugeTest.helper.createTestDataValue(true, i);
			String[] result = this.okuyamaClient.getValue(testDataKey);
			if (result[0].equals("true")) {
				assertEquals(result[1], testDataValue);
			} else {
				fail("getメソッドエラー");
			}
		}
	}

}

package test.junit;

import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * okuyamaのメソッドを1回テストするためのクラス。
 * @author s-ito
 *
 */
public class MethodTest {

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private int start;

	private int count = 5000;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		MethodTest.helper.init();
		// okuyamaに接続
		this.okuyamaClient =  MethodTest.helper.getConnectedOkuyamaClient();
		// 変数初期化
		this.count = 5000;
		this.start = MethodTest.helper.getStart();
		this.count += this.start;
	}

	@After
	public void tearDown() throws Exception {
		// okuyamaとの通信を切断
		this.okuyamaClient.close();
	}
/*
	@Test
	public void testSet() throws Exception {
		String[] testData;
		for (int i = start; i < count; i++) {
			// データ登録
			testData = MethodTest.helper.getTestData(i, false);
			assertTrue("Set - Error=[" + testData[0] + ", " + testData[1],
						okuyamaClient.setValue(testData[0], testData[1]));
		}
		for (int i = start; i < start+500; i++) {
			// データ登録
			testData = MethodTest.helper.getTestData(i, true);
			assertTrue("Set - Error=[" + testData[0] + ", " + testData[1],
						okuyamaClient.setValue(testData[0], testData[1]));
		}
	}

	@Test
	public void testGet() throws Exception {
		String[] testData;
		// テストデータを入れる
		for (int i = start; i < count; i++) {
			// データ登録
			testData = MethodTest.helper.getTestData(i, false);
			if (!okuyamaClient.setValue(testData[0], testData[1])) {
				fail("getメソッドテスト準備失敗");
			}
		}
		for (int i = start; i < start+500; i++) {
			// データ登録
			testData = MethodTest.helper.getTestData(i, true);
			if (!okuyamaClient.setValue(testData[0], testData[1])) {
				fail("getメソッドテスト準備失敗");
			}
		}
		// 1つずつ取得
		String[] ret = null;
		for (int i = start; i < count; i++) {
			testData = MethodTest.helper.getTestData(i, false);
			ret = okuyamaClient.getValue(testData[0]);
			if (ret[0].equals("true")) {
				assertEquals("データが合っていない key=[" + testData[0] + "]  value=[" + ret[1] + "]",
							ret[1], testData[1]);
			} else if (ret[0].equals("false")) {
				fail("データなし key=[" + testData[0] + "]");
			} else if (ret[0].equals("error")) {
				fail("Error key=[" + testData[0] + "]" + ret[1]);
			}
		}
		for (int i = start; i < start+500; i++) {
			testData = MethodTest.helper.getTestData(i, true);
			ret = okuyamaClient.getValue(testData[0]);
			if (ret[0].equals("true")) {
				assertEquals("データが合っていない key=[" + testData[0] + "]  value=[" + ret[1] + "]",
							ret[1], testData[1]);
			} else if (ret[0].equals("false")) {
				fail("データなし key=[" + testData[0] + "]");
			} else if (ret[0].equals("error")) {
				fail("Error key=[" + testData[0] + "]" + ret[1]);
			}
		}
		// 一括で取得
		String[] keys = new String[102];
		int idx = 0;
		Map<String, String> checkResultMap = new HashMap<>(50);
		for (int i = start; i < start+50; i++) {
			testData = MethodTest.helper.getTestData(i, false);
			keys[idx] = testData[0];
			checkResultMap.put(keys[idx], testData[1]);
			testData = MethodTest.helper.getTestData(i, true);
			keys[idx+1] = testData[0];
			checkResultMap.put(keys[idx+1], testData[1]);
			idx = idx+2;
		}
		Map<?, ?> multiGetRet = okuyamaClient.getMultiValue(keys);
		assertEquals("データなし MultiGet=[" + multiGetRet + "]", checkResultMap, multiGetRet);
	}
	*/

}

package test.junit.simple;

import static org.junit.Assert.*;

import java.util.Map;

import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * getメソッドによる複数同時取得の簡単なテスト。
 * @author s-ito
 *
 */
public class MultiGetMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String[] testDataKey;

	private String[] testDataValue;

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		MultiGetMethodSimpleTest.helper.init();
		MultiGetMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  MultiGetMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		String testKey = MultiGetMethodSimpleTest.helper.createTestDataKey(false);
		String testValue = MultiGetMethodSimpleTest.helper.createTestDataValue(false);
		this.testDataKey = new String[] {
				testKey + 1, testKey + 2, testKey + 3
		};
		this.testDataValue = new String[] {
				testValue + 1, testValue + 2, testValue + 3
		};
		this.okuyamaClient.setValue(this.testDataKey[0], this.testDataValue[0]);
		this.okuyamaClient.setValue(this.testDataKey[1], this.testDataValue[1]);
		this.okuyamaClient.setValue(this.testDataKey[2], this.testDataValue[2]);
	}

	@After
	public void tearDown() throws Exception {
		// テストデータを破棄
		try {
			this.okuyamaClient.removeValue(this.testDataKey[0]);
			this.okuyamaClient.removeValue(this.testDataKey[1]);
			this.okuyamaClient.removeValue(this.testDataKey[2]);
		} catch (OkuyamaClientException e) {
		}
		this.okuyamaClient.close();
	}

	@Test
	public void キーに対応した値を取得する() throws Exception {
		Map<?, ?> result = this.okuyamaClient.getMultiValue(this.testDataKey);
		assertEquals(result.get(this.testDataKey[0]), this.testDataValue[0]);
		assertEquals(result.get(this.testDataKey[1]), this.testDataValue[1]);
		assertEquals(result.get(this.testDataKey[2]), this.testDataValue[2]);
	}

	@Test
	public void 存在しないキーを含んだ() throws Exception {
		String[] result = this.okuyamaClient.getValue(this.testDataKey[0] + "_foo");
		assertEquals(result[0], "false");
	}

	@Test
	public void nullのキーを指定して例外を発生させる() throws Exception {
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage("The blank is not admitted on a key");
		this.okuyamaClient.getValue(null);
	}

	@Test
	public void サーバとのセッションが無い状態でgetすることで例外を発生させる() throws Exception {
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage("No ServerConnect!!");
		this.okuyamaClient.removeValue(this.testDataKey[0]);
		this.okuyamaClient.removeValue(this.testDataKey[1]);
		this.okuyamaClient.removeValue(this.testDataKey[2]);
		this.okuyamaClient.close();
		this.okuyamaClient.getValue(this.testDataKey[0]);
		this.okuyamaClient.getValue(this.testDataKey[0]);
		this.okuyamaClient.getValue(this.testDataKey[0]);
	}

}

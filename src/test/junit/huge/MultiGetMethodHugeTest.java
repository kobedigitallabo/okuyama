package test.junit.huge;

import static org.junit.Assert.*;

import java.util.Map;

import okuyama.imdst.client.OkuyamaClient;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * 巨大データに対するgetメソッドによる複数同時取得の簡単なテスト。
 * @author s-ito
 *
 */
public class MultiGetMethodHugeTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String[] testDataKey;

	private String[] testDataValue;

	@Before
	public void setUp() throws Exception {
		MultiGetMethodHugeTest.helper.init();
		MultiGetMethodHugeTest.helper.initBigTestData();
		// okuyamaに接続
		this.okuyamaClient =  MultiGetMethodHugeTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		String testKey = MultiGetMethodHugeTest.helper.createTestDataKey(true);
		String testValue = MultiGetMethodHugeTest.helper.createTestDataValue(true);
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
		MultiGetMethodHugeTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void キーに対応した巨大な値を複数同時に取得する() throws Exception {
		Map<?, ?> result = this.okuyamaClient.getMultiValue(this.testDataKey);
		assertEquals(result.get(this.testDataKey[0]), this.testDataValue[0]);
		assertEquals(result.get(this.testDataKey[1]), this.testDataValue[1]);
		assertEquals(result.get(this.testDataKey[2]), this.testDataValue[2]);
	}

}

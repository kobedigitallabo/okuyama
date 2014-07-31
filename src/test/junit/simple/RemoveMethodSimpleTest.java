package test.junit.simple;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * removeメソッドの簡単なテスト。
 * @author s-ito
 *
 */
public class RemoveMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String testDataKey;

	private String testDataValue;

	@Before
	public void setUp() throws Exception {
		RemoveMethodSimpleTest.helper.init();
		RemoveMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  RemoveMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// テストデータを設定
		this.testDataKey = RemoveMethodSimpleTest.helper.createTestDataKey(false);
		this.testDataValue = RemoveMethodSimpleTest.helper.createTestDataValue(false);
		this.okuyamaClient.setValue(this.testDataKey, this.testDataValue);
		this.okuyamaClient.setValue(this.testDataKey + "日本語", this.testDataValue + "日本語");
	}

	@After
	public void tearDown() throws Exception {
		RemoveMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void 登録されたデータを削除する() throws Exception {
		String[] result = this.okuyamaClient.removeValue(this.testDataKey);
		if (result[0].equals("true")) {
			assertEquals(result[1], this.testDataValue);
		} else {
			fail("removeメソッドエラー");
		}
	}

	@Test
	public void 登録されたキーにマルチバイト文字列を含むデータを削除する() throws Exception {
		String[] result = this.okuyamaClient.removeValue(this.testDataKey + "日本語");
		if (result[0].equals("true")) {
			assertEquals(result[1], this.testDataValue + "日本語");
		} else {
			fail("removeメソッドエラー");
		}
	}

	@Test
	public void 登録されていないデータを削除しようとして失敗する() throws Exception {
		String[] result = this.okuyamaClient.removeValue(this.testDataKey + "_nothing");
		assertEquals(result[0], "false");
	}

	@Test
	public void 削除済みのデータを再度削除しようとして失敗する() throws Exception {
		String[] result = this.okuyamaClient.removeValue(this.testDataKey);
		result = this.okuyamaClient.removeValue(this.testDataKey);
		assertEquals(result[0], "false");
	}

	@Test
	public void 削除済みのデータを取得しようとしてエラーを起こす() throws Exception {
		String[] result = this.okuyamaClient.removeValue(this.testDataKey);
		result = this.okuyamaClient.getValue(this.testDataKey);
		assertEquals(result[0], "false");
	}

	@Test
	public void サーバとのセッションが無い状態でgetすることで例外を発生させる() throws Exception {
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage("No ServerConnect!!");
		this.okuyamaClient.close();
		this.okuyamaClient.getValue(this.testDataKey);
	}

}

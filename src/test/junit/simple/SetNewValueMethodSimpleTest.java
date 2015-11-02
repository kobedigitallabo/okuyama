package test.junit.simple;

import static org.junit.Assert.*;

import java.util.Random;

import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import test.junit.MethodTestHelper;

/**
 * setNewValueメソッドの簡単なテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class SetNewValueMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		SetNewValueMethodSimpleTest.helper.init();
		SetNewValueMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetNewValueMethodSimpleTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
		SetNewValueMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void データを新規登録する() throws Exception {
		String[] result = this.okuyamaClient.setNewValue(SetNewValueMethodSimpleTest.helper.createTestDataKey(false), 
														SetNewValueMethodSimpleTest.helper.createTestDataValue(false));
		assertEquals(result[0], "true");
	}
	
	@Test
	public void 登録済みKeyに対してデータを新規登録しようとして失敗する() throws Exception {
		String key = SetNewValueMethodSimpleTest.helper.createTestDataKey(false);
		String value = SetNewValueMethodSimpleTest.helper.createTestDataValue(false);
		String[] result = this.okuyamaClient.setNewValue(key, value);
		assertEquals(result[0], "true");
		result = this.okuyamaClient.setNewValue(key, value);
		assertEquals(result[0], "false");
	}
	
	@Test
	public void マルチバイト文字列が含まれたキーと値を新規登録する() throws Exception {
		String[] result;
		result = this.okuyamaClient.setNewValue(SetNewValueMethodSimpleTest.helper.createTestDataKey(false) + "日本語",
												SetNewValueMethodSimpleTest.helper.createTestDataValue(false));
		assertEquals(result[0], "true");
	}

	@Test
	public void 値をObjectとして新規登録する() throws Exception {
		String[] result;
		result = this.okuyamaClient.setNewObjectValue(SetNewValueMethodSimpleTest.helper.createTestDataKey(false),
														SetNewValueMethodSimpleTest.helper.createTestDataValue(false));
		assertEquals(result[0], "true");
	}

	@Test
	public void nullのキーを新規登録して例外を発生させる() throws Exception {
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage("The blank is not admitted on a key");
		this.okuyamaClient.setNewValue(null, "foo");
	}

	@Test
	public void 文字数が328以上のキーを新規登録して例外を発生させる() throws Exception {
		// 文字数が328のキーを作成
		final int keySize = 328;
		Random rnd = new Random();
		StringBuilder bigKeyBuilder = new StringBuilder(keySize);
		for (int i = 0;i < keySize;i++) {
			bigKeyBuilder = bigKeyBuilder.append(rnd.nextInt(10));
		}
		// テスト
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage("Save Key Max Size 327 Byte");
		this.okuyamaClient.setNewValue(bigKeyBuilder.toString(), "foo");
	}

	@Test
	public void サーバとのセッションが無い状態でsetして例外を発生させる() throws Exception {
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage("No ServerConnect!!");
		this.okuyamaClient.close();
		this.okuyamaClient.setNewValue("foo", "bar");
	}

}

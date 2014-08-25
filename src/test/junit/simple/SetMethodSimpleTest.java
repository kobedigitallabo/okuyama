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
 * setメソッドの簡単なテスト。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class SetMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		SetMethodSimpleTest.helper.init();
		SetMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetMethodSimpleTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
		SetMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void キーと値をsetする() throws Exception {
		assertTrue(this.okuyamaClient.setValue(SetMethodSimpleTest.helper.createTestDataKey(false),
												SetMethodSimpleTest.helper.createTestDataValue(false)));
	}

	@Test
	public void マルチバイト文字列が含まれたキーと値をsetする() throws Exception {
		assertTrue(this.okuyamaClient.setValue(SetMethodSimpleTest.helper.createTestDataKey(false) + "日本語",
												SetMethodSimpleTest.helper.createTestDataValue(false)));
	}

	@Test
	public void 値をObjectとしてsetする() throws Exception {
		assertTrue(this.okuyamaClient.setObjectValue(SetMethodSimpleTest.helper.createTestDataKey(false),
														SetMethodSimpleTest.helper.createTestDataValue(false)));
	}

	@Test
	public void nullのキーをsetして例外を発生させる() throws Exception {
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage("The blank is not admitted on a key");
		this.okuyamaClient.setValue(null, "foo");
	}

	@Test
	public void 文字数が328以上のキーをsetして例外を発生させる() throws Exception {
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
		this.okuyamaClient.setValue(bigKeyBuilder.toString(), "foo");
	}

	@Test
	public void サーバとのセッションが無い状態でsetして例外を発生させる() throws Exception {
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage("No ServerConnect!!");
		this.okuyamaClient.close();
		this.okuyamaClient.setValue("foo", "bar");
	}

}

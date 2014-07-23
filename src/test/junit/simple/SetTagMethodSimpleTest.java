package test.junit.simple;

import static org.junit.Assert.*;

import java.io.UnsupportedEncodingException;
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
 * setTagメソッドの簡単なテスト。
 * @author s-ito
 *
 */
public class SetTagMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	@Before
	public void setUp() throws Exception {
		SetTagMethodSimpleTest.helper.init();
		SetTagMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  SetTagMethodSimpleTest.helper.getConnectedOkuyamaClient();
	}

	@After
	public void tearDown() throws Exception {
		SetTagMethodSimpleTest.helper.deleteAllData();
		this.okuyamaClient.close();
	}

	@Test
	public void データにタグを1つ付けてsetする() throws Exception {
		assertTrue(this.okuyamaClient.setValue(SetTagMethodSimpleTest.helper.createTestDataKey(false),
												new String[]{SetTagMethodSimpleTest.helper.createTestDataTag()},
												SetTagMethodSimpleTest.helper.createTestDataValue(false)));
	}

	@Test
	public void データにマルチバイト文字列が含まれたタグを1つ付けてsetする() throws Exception {
		assertTrue(this.okuyamaClient.setValue(SetTagMethodSimpleTest.helper.createTestDataKey(false),
											new String[]{SetTagMethodSimpleTest.helper.createTestDataTag() + "日本語"},
											SetTagMethodSimpleTest.helper.createTestDataValue(false)));
	}

	@Test
	public void nullのタグをsetして例外を発生させる() throws Exception {
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage(NullPointerException.class.getName());
		this.okuyamaClient.setValue(SetTagMethodSimpleTest.helper.createTestDataKey(false),
									new String[]{null},
									SetTagMethodSimpleTest.helper.createTestDataValue(false));
	}

	@Test
	public void タグとしてnullなString配列をsetする() throws Exception {
		String[] nullArray = null;
		assertTrue(this.okuyamaClient.setValue(SetTagMethodSimpleTest.helper.createTestDataKey(false),
									nullArray,
									SetTagMethodSimpleTest.helper.createTestDataValue(false)));
		String[] result = this.okuyamaClient.getValue(SetTagMethodSimpleTest.helper.createTestDataKey(false));
		assertEquals(result[0], "true");
	}

	@Test
	public void タグとして要素無しのString配列をsetする() throws Exception {
		assertTrue(this.okuyamaClient.setValue(SetTagMethodSimpleTest.helper.createTestDataKey(false),
									new String[0],
									SetTagMethodSimpleTest.helper.createTestDataValue(false)));
		String[] result = this.okuyamaClient.getValue(SetTagMethodSimpleTest.helper.createTestDataKey(false));
		assertEquals(result[0], "true");
	}

	@Test
	public void タグ10個を1つのデータにsetする() throws Exception {
		String[] tags = new String[]{
				SetTagMethodSimpleTest.helper.createTestDataTag(1),
				SetTagMethodSimpleTest.helper.createTestDataTag(2),
				SetTagMethodSimpleTest.helper.createTestDataTag(3),
				SetTagMethodSimpleTest.helper.createTestDataTag(4),
				SetTagMethodSimpleTest.helper.createTestDataTag(5),
				SetTagMethodSimpleTest.helper.createTestDataTag(6),
				SetTagMethodSimpleTest.helper.createTestDataTag(7),
				SetTagMethodSimpleTest.helper.createTestDataTag(8),
				SetTagMethodSimpleTest.helper.createTestDataTag(9),
				SetTagMethodSimpleTest.helper.createTestDataTag(10)
		};
		assertTrue(this.okuyamaClient.setValue(SetTagMethodSimpleTest.helper.createTestDataKey(false),
												tags,
												SetTagMethodSimpleTest.helper.createTestDataValue(false)));
	}

	@Test
	public void 文字数が328以上のタグをsetして例外を発生させる() throws Exception {
		// 文字数が328のキーを作成
		final int tagSize = 328;
		Random rnd = new Random();
		StringBuilder bigTagBuilder = new StringBuilder(tagSize);
		for (int i = 0;i < tagSize;i++) {
			bigTagBuilder = bigTagBuilder.append(rnd.nextInt(10));
		}
		// テスト
		thrown.expect(OkuyamaClientException.class);
		thrown.expectMessage(UnsupportedEncodingException.class.getName());
		this.okuyamaClient.setValue(SetTagMethodSimpleTest.helper.createTestDataKey(false),
									bigTagBuilder.toString(),
									SetTagMethodSimpleTest.helper.createTestDataValue(false));
	}

}

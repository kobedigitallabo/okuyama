package test.junit.simple;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
 * getMultiTagValuesとgetMultiTagKeysの簡単なテスト。
 * @author s-ito
 *
 */
public class MultiGetTagMethodSimpleTest {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	private static MethodTestHelper helper = new MethodTestHelper();

	private OkuyamaClient okuyamaClient;

	private String[][] testTags = new String[5][];

	private Map<Object, String> answerValueList = new HashMap<Object, String>();

	private Map<String, List<String>> answerKeyList = new HashMap<String, List<String>>();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
	}

	@AfterClass
	public static void tearDownAfterClass() throws Exception {
	}

	@Before
	public void setUp() throws Exception {
		MultiGetTagMethodSimpleTest.helper.init();
		MultiGetTagMethodSimpleTest.helper.initTestData();
		// okuyamaに接続
		this.okuyamaClient =  MultiGetTagMethodSimpleTest.helper.getConnectedOkuyamaClient();
		// タグ作成
		String[] tags = new String[4];
		for (int i = 0;i < 4;i++) {
			tags[i] = MultiGetTagMethodSimpleTest.helper.createTestDataTag(i);
			this.answerKeyList.put(tags[i], new ArrayList<String>());
		}
		this.testTags[0] = new String[]{tags[0]};
		this.testTags[1] = new String[]{tags[0], tags[1]};
		this.testTags[2] = new String[]{tags[0], tags[1], tags[2]};
		this.testTags[3] = new String[]{tags[3]};
		this.testTags[4] = new String[]{tags[0], tags[3]};
		// テストデータを入力
		for (int i = 0; i < 100; i++) {
			String[] testTags = this.testTags[i % 4];
			String key = MultiGetTagMethodSimpleTest.helper.createTestDataKey(false, i);
			String value = MultiGetTagMethodSimpleTest.helper.createTestDataValue(false, i);
			this.answerValueList.put(key, value);
			this.okuyamaClient.setValue(key, this.testTags[i % 4], value);
			for (String tag : testTags) {
				this.answerKeyList.get(tag).add(key);
			}
        }
		for (String tag : tags) {
			Collections.sort(this.answerKeyList.get(tag));
		}
	}

	@After
	public void tearDown() throws Exception {
		for (int i = 0;i < 100;i++) {
			String key = MultiGetTagMethodSimpleTest.helper.createTestDataKey(false, i);
			String[] tags = this.testTags[i % 4];
			try {
				for (int j = 0;j < tags.length;j++) {
					this.okuyamaClient.removeTagFromKey(key, tags[j]);
				}
				this.okuyamaClient.removeValue(key);
			} catch (OkuyamaClientException e) {
			}
		}
		this.okuyamaClient.close();
	}

	@Test
	public void タグ1つに紐付いている複数の値を取得する() throws Exception {
		Map<?, ?> result = this.okuyamaClient.getMultiTagValues(this.testTags[0]);
		assertEquals(result.size(), 75);
		for (Object key : result.keySet()) {
			assertEquals(result.get(key), this.answerValueList.get(key));
		}
	}

	@Test
	public void タグ値のリストのタグ全てに紐付いている指定したタグ以外のタグを持つものを含む複数の値を取得する()
																									throws Exception {
		Map<?, ?> result = this.okuyamaClient.getMultiTagValues(this.testTags[1], true);
		assertEquals(result.size(), 50);
		for (Object key : result.keySet()) {
			assertEquals(result.get(key), this.answerValueList.get(key));
		}
	}

	@Test
	public void タグ値のリストのタグ全てに紐付いている複数の値を取得する() throws Exception {
		Map<?, ?> result = this.okuyamaClient.getMultiTagValues(this.testTags[2], true);
		assertEquals(result.size(), 25);
		for (Object key : result.keySet()) {
			assertEquals(result.get(key), this.answerValueList.get(key));
		}
	}

	@Test
	public void タグ値のリストのタグのどれかに紐付いている複数の値を取得する() throws Exception {
		Map<?, ?> result = this.okuyamaClient.getMultiTagValues(this.testTags[4], false);
		assertEquals(result.size(), 100);
		for (Object key : result.keySet()) {
			assertEquals(result.get(key), this.answerValueList.get(key));
		}
	}

	@Test
	public void タグ1つに紐付いている複数のキーを取得する() throws Exception {
		String[] tags = this.testTags[0];
		String[] result = this.okuyamaClient.getMultiTagKeys(tags);
		assertEquals(result.length, 75);
		for (String tag : tags) {
			for (String key : result) {
				assertTrue(this.existKeyInTag(tag, key));
			}
		}
	}
	@Test
	public void タグ値のリストのタグ全てに紐付いている指定したタグ以外のタグを持つものを含む複数のキーを取得する()
																									throws Exception {
		String[] tags = this.testTags[0];
		String[] result = this.okuyamaClient.getMultiTagKeys(tags);
		assertEquals(result.length, 75);
		for (String tag : tags) {
			for (String key : result) {
				assertTrue(this.existKeyInTag(tag, key));
			}
		}
	}

	@Test
	public void タグ値のリストのタグ全てに紐付いている複数のキーを取得する() throws Exception {
		String[] tags = this.testTags[0];
		String[] result = this.okuyamaClient.getMultiTagKeys(tags);
		assertEquals(result.length, 75);
		for (String tag : tags) {
			for (String key : result) {
				assertTrue(this.existKeyInTag(tag, key));
			}
		}
	}

	@Test
	public void タグ値のリストのタグのどれかに紐付いている複数のキーを取得する() throws Exception {
		String[] tags = this.testTags[0];
		String[] result = this.okuyamaClient.getMultiTagKeys(tags);
		assertEquals(result.length, 75);
		for (String tag : tags) {
			for (String key : result) {
				assertTrue(this.existKeyInTag(tag, key));
			}
		}
	}

	private boolean existKeyInTag(String tag, String key) {
		List<String> keys = this.answerKeyList.get(tag);
		for (String k : keys) {
			if (k.equals(key)) {
				return true;
			}
		}
		return false;
	}
}

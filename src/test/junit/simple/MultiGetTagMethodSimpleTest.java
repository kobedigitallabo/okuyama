package test.junit.simple;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;
import okuyama.imdst.client.OkuyamaResultSet;

import org.junit.After;
import org.junit.Before;
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

	private Map<String, String[]> keyTags = new HashMap<String, String[]>();

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
			this.keyTags.put(key, testTags);
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
		ArrayList<String> keys = new ArrayList<String>();
		for (Object key : result.keySet()) {
			keys.add((String) key);
			assertEquals(result.get(key), this.answerValueList.get(key));
		}
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, this.testTags[0],
													keys.toArray(new String[keys.size()]), true));
	}

	@Test
	public void タグ値のリストのタグ全てに紐付いている指定したタグ以外のタグを持つものを含む複数の値を取得する()
																									throws Exception {
		Map<?, ?> result = this.okuyamaClient.getMultiTagValues(this.testTags[1], true);
		assertEquals(result.size(), 50);
		ArrayList<String> keys = new ArrayList<String>();
		for (Object key : result.keySet()) {
			keys.add((String) key);
			assertEquals(result.get(key), this.answerValueList.get(key));
		}
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, this.testTags[1],
													keys.toArray(new String[keys.size()]), true));
	}

	@Test
	public void タグ値のリストのタグ全てに紐付いている複数の値を取得する() throws Exception {
		Map<?, ?> result = this.okuyamaClient.getMultiTagValues(this.testTags[2], true);
		assertEquals(result.size(), 25);
		ArrayList<String> keys = new ArrayList<String>();
		for (Object key : result.keySet()) {
			keys.add((String) key);
			assertEquals(result.get(key), this.answerValueList.get(key));
		}
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, this.testTags[2],
													keys.toArray(new String[keys.size()]), true));
	}

	@Test
	public void タグ値のリストのタグのどれかに紐付いている複数の値を取得する() throws Exception {
		Map<?, ?> result = this.okuyamaClient.getMultiTagValues(this.testTags[4], false);
		assertEquals(result.size(), 100);
		ArrayList<String> keys = new ArrayList<String>();
		for (Object key : result.keySet()) {
			keys.add((String) key);
			assertEquals(result.get(key), this.answerValueList.get(key));
		}
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, this.testTags[4],
													keys.toArray(new String[keys.size()]), false));
	}

	@Test
	public void タグ1つに紐付いている複数のキーを取得する() throws Exception {
		String[] tags = this.testTags[0];
		String[] result = this.okuyamaClient.getMultiTagKeys(tags);
		assertEquals(result.length, 75);
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, this.testTags[0], result, true));
	}
	@Test
	public void タグ値のリストのタグ全てに紐付いている指定したタグ以外のタグを持つものを含む複数のキーを取得する()
																									throws Exception {
		String[] tags = this.testTags[1];
		String[] result = this.okuyamaClient.getMultiTagKeys(tags);
		assertEquals(result.length, 50);
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, this.testTags[1], result, true));
	}

	@Test
	public void タグ値のリストのタグ全てに紐付いている複数のキーを取得する() throws Exception {
		String[] tags = this.testTags[2];
		String[] result = this.okuyamaClient.getMultiTagKeys(tags);
		assertEquals(result.length, 25);
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, this.testTags[2], result, true));
	}

	@Test
	public void タグ値のリストのタグのどれかに紐付いている複数のキーを取得する() throws Exception {
		String[] tags = this.testTags[4];
		String[] result = this.okuyamaClient.getMultiTagKeys(tags, false);
		assertEquals(result.length, 100);
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, this.testTags[4], result, false));
	}

	@Test
	public void OkuyamaResultSetでタグ1つに紐付いている複数のキーを取得する() throws Exception {
		String[] tags = this.testTags[0];
		OkuyamaResultSet resultSet = this.okuyamaClient.getMultiTagKeysResult(tags);
		ArrayList<String> keys = new ArrayList<String>();
		while (resultSet.next()) {
			String key = (String) resultSet.getKey();
			keys.add(key);
			assertEquals(resultSet.getValue(), this.answerValueList.get(key));
		}
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, tags, keys.toArray(new String[keys.size()]), true));
	}
	@Test
	public void OkuyamaResultSetでタグ値のリストのタグ全てに紐付いている指定したタグ以外のタグを持つものを含む複数のキーを取得する()
																									throws Exception {
		String[] tags = this.testTags[1];
		OkuyamaResultSet resultSet = this.okuyamaClient.getMultiTagKeysResult(tags, true);
		ArrayList<String> keys = new ArrayList<String>();
		while (resultSet.next()) {
			String key = (String) resultSet.getKey();
			keys.add(key);
			assertEquals(resultSet.getValue(), this.answerValueList.get(key));
		}
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, tags, keys.toArray(new String[keys.size()]), true));
	}

	@Test
	public void OkuyamaResultSetでタグ値のリストのタグ全てに紐付いている複数のキーを取得する() throws Exception {
		String[] tags = this.testTags[2];
		OkuyamaResultSet resultSet = this.okuyamaClient.getMultiTagKeysResult(tags, true);
		ArrayList<String> keys = new ArrayList<String>();
		while (resultSet.next()) {
			String key = (String) resultSet.getKey();
			keys.add(key);
			assertEquals(resultSet.getValue(), this.answerValueList.get(key));
		}
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, tags, keys.toArray(new String[keys.size()]), true));
	}

	@Test
	public void OkuyamaResultSetでタグ値のリストのタグのどれかに紐付いている複数のキーを取得する() throws Exception {
		String[] tags = this.testTags[4];
		OkuyamaResultSet resultSet = this.okuyamaClient.getMultiTagKeysResult(tags, false);
		ArrayList<String> keys = new ArrayList<String>();
		while (resultSet.next()) {
			String key = (String) resultSet.getKey();
			keys.add(key);
			assertEquals(resultSet.getValue(), this.answerValueList.get(key));
		}
		assertTrue(MethodTestHelper.checkTagResult(this.keyTags, tags, keys.toArray(new String[keys.size()]), false));
	}
}

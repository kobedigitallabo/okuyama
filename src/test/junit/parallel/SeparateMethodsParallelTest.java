package test.junit.parallel;

import static org.junit.Assert.*;
import okuyama.imdst.client.OkuyamaClient;
import okuyama.imdst.client.OkuyamaClientException;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import test.junit.MethodTestHelper;
import test.junit.MultiThreadRule;

public class SeparateMethodsParallelTest {

	@Rule
	public MultiThreadRule thread = new MultiThreadRule(100);

	private static MethodTestHelper helper = new MethodTestHelper();

	@BeforeClass
	public static void setUpBeforeClass() throws Exception {
		SeparateMethodsParallelTest.helper.init();
		SeparateMethodsParallelTest.helper.initTestData();
	}
	
	@AfterClass
	public static void tearDownAfterClass() throws Exception {
		SeparateMethodsParallelTest.helper.deleteAllData();
	}

	@Before
	public void setUp() throws Exception {
		long id = Thread.currentThread().getId();
		// スレッドIDが偶数の場合はテストでsetをするのでここでのset処理は省く
		if ((id % 2) == 0) {
			Thread.sleep(5);
			return;
		}
		OkuyamaClient client = SeparateMethodsParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			client.setValue(SeparateMethodsParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id,
							SeparateMethodsParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id);
		}
		client.close();
	}

	@After
	public void tearDown() throws Exception {
		long id = Thread.currentThread().getId();
		OkuyamaClient client = SeparateMethodsParallelTest.helper.getConnectedOkuyamaClient();
		for (int i = 0;i < 50;i++) {
			try {
				client.removeValue(SeparateMethodsParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id);
			} catch (OkuyamaClientException e) {
			}
		}
		client.close();
	}

	@Test
	public void 別々のデータに対してsetとgetを同時に行う() throws Exception {
		OkuyamaClient client = SeparateMethodsParallelTest.helper.getConnectedOkuyamaClient();
		long id = Thread.currentThread().getId();
		if ((id % 2) == 0) {
			for (int i = 0;i < 50;i++) {
				assertTrue(client.setValue(SeparateMethodsParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id ,
											SeparateMethodsParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id));
			}
		} else {
			for (int i = 0;i < 50;i++) {
				String testDataKey = SeparateMethodsParallelTest.helper.createTestDataKey(false, i) + "_thread_" + id;
				String testDataValue = SeparateMethodsParallelTest.helper.createTestDataValue(false, i) + "_thread_" + id;
				String[] result = client.getValue(testDataKey);
				if (result[0].equals("true")) {
					assertEquals(result[1], testDataValue);
				} else {
					fail("getメソッドエラー");
				}
			}
		}
		client.close();
	}

}

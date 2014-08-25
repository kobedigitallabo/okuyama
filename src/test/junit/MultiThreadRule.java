package test.junit;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.junit.rules.MethodRule;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.Statement;

/**
 * テストをマルチスレッドで行うためのクラス。
 * 
 * @author T.Okuyama
 * @license GPL(Lv3)
 *
 */
public class MultiThreadRule implements MethodRule {
	
	/**
	 * 起動するスレッドの数。
	 */
	private int threadNum;

	/**
	 * コンストラクタ。
	 */
	public MultiThreadRule(int threadNum) {
		this.threadNum = threadNum;
	}

	@Override
	public Statement apply(final Statement base, FrameworkMethod method, Object target) {
		final Runnable run = new Runnable() {
			@Override
			public void run() {
				try {
					base.evaluate();
				} catch (Throwable t) {
					throw new RuntimeException(t);
				}
			}
		};
		return new Statement() {
			@Override
			public void evaluate() throws Throwable {
				ExecutorService es = Executors.newFixedThreadPool(threadNum);
				Future<?>[] fa = new Future<?>[threadNum];
				// 別スレッドでテストを実行する
				for (int i = 0;i < threadNum;i++) {
					fa[i] = es.submit(run);
				}
				// 全スレッドのテストが終了するまで待機
				for (Future<?> f : fa) {
					f.get();
				}
				es.shutdown();
			}
		};
	}

}

package test.helper.manager;

import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.ConsoleHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;

/**
 * okuyamaテスト用ログ処理クラス。
 * @author s-ito
 *
 */
public class OkuyamaManagerConsoleHandler extends ConsoleHandler {

	public OkuyamaManagerConsoleHandler() {
		super();
	}

	@Override
	public void publish(LogRecord record) {
		if (Level.WARNING.equals(record.getLevel()) || Level.SEVERE.equals(record.getLevel())) {
			this.setOutputStream(new UncloseOutputStream(System.err));
		} else {
			this.setOutputStream(new UncloseOutputStream(System.out));
		}
		super.publish(record);
		flush();
	}

	/**
	 * 閉じられないOutputStream
	 * @author s-ito
	 *
	 */
	public class UncloseOutputStream extends OutputStream {

		private OutputStream stream;

		public UncloseOutputStream(OutputStream stream) {
			this.stream = stream;
		}

		@Override
		public void write(int b) throws IOException {
			this.stream.write(b);
		}

		@Override
		public void write(byte b[]) throws IOException {
			this.stream.write(b);
		}

		@Override
		public void write(byte b[], int off, int len) throws IOException {
			this.stream.write(b, 0, b.length);
		}

		@Override
		public void flush() throws IOException {
			this.stream.flush();
		}

		@Override
		public void close() throws IOException {
		}
	}
}

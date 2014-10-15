package test.helper.manager;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.logging.Logger;

/**
 * ローカル上のOkuyama操作用端末。
 * @author s-ito
 *
 */
public abstract class LocalOkuyamaTerminal implements OkuyamaTerminal {

	private Logger logger = Logger.getLogger(LocalOkuyamaTerminal.class.getName());
	
	@Override
	public boolean mkdir(String path) {
		this.logger.fine("mkdir \"" + path + "\"");
		return new File(path).mkdir();
	}

	@Override
	public boolean delete(String path) {
		this.logger.fine("delete \"" + path + "\"");
		return new File(path).delete();
	}

	@Override
	public String[] ls(String path) {
		this.logger.fine("ls \"" + path + "\"");
		return new File(path).list();
	}
	
	@Override
	public boolean exist(String path) {
		return new File(path).exists();
	}
	
	@Override
	public String getParentPath(String path) {
		return new File(path).getParent();
	}
	
	public String concatPath(String parent, String child) {
		return new File(parent, child).getPath();
	}

	@Override
	public InputStream load(String path) throws IOException {
		this.logger.fine("load \"" + path + "\"");
		return new FileInputStream(path);
	}

	@Override
	public OutputStream write(String path) throws IOException {
		this.logger.fine("write \"" + path + "\"");
		return new FileOutputStream(path);
	}

}

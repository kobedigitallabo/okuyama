package fuse.okuyamafs;

/**
 * OkuyamaFuse.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class JVMShutdownSequence extends Thread {

    public void run() {
        OkuyamaFilesystem.jvmShutdownStatus = true;
        System.out.println("shutdown...");
    }
}


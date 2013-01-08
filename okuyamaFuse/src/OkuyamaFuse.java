package fuse.okuyamafs;


import java.io.*;
import java.nio.*;
import java.util.*;


import fuse.*;

import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import okuyama.imdst.util.*;

/**
 * OkuyamaFuse.<br>
 * 
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class OkuyamaFuse {

    private static final Log log = LogFactory.getLog(OkuyamaFuse.class);

    public static void main(String[] args) {

        String fuseArgs[] = new String[args.length - 1];
        System.arraycopy(args, 0, fuseArgs, 0, fuseArgs.length);

        ImdstDefine.valueCompresserLevel = 9;
        try {
            String okuyamaStr = args[args.length - 1];
            String[] masterNodeInfos = null;
            if (okuyamaStr.indexOf(",") != -1) {
                masterNodeInfos = okuyamaStr.split(",");
            } else {
                masterNodeInfos = (okuyamaStr + "," + okuyamaStr).split(",");
            }
            // 1=Memory
            // 2=okuyama
            // 3=LocalCacheOkuyama


            String[] optionParams = {"2","true"};
            String fsystemMode = optionParams[0].trim();
            boolean singleFlg = new Boolean(optionParams[1].trim()).booleanValue();

            OkuyamaFilesystem.storageType = new Integer(fsystemMode).intValue();
            if (OkuyamaFilesystem.storageType == 1) OkuyamaFilesystem.blockSize = OkuyamaFilesystem.blockSize;

            CoreMapFactory.init(new Integer(fsystemMode.trim()).intValue(), masterNodeInfos);
            FilesystemCheckDaemon loopDaemon = new FilesystemCheckDaemon(1, fuseArgs[fuseArgs.length - 1]);
            loopDaemon.start();

            if (OkuyamaFilesystem.storageType == 2) {
                FilesystemCheckDaemon bufferCheckDaemon = new FilesystemCheckDaemon(2, null);
                bufferCheckDaemon.start();
            }

            FuseMount.mount(fuseArgs, new OkuyamaFilesystem(fsystemMode, singleFlg), log);
        } catch (Exception e) {
           e.printStackTrace();
        }
    }
}


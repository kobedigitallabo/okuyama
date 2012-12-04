package fuse.okuyamafs;

import fuse.*;
//import fuse.compat.Filesystem2;
//import fuse.compat.FuseDirEnt;
//import fuse.compat.FuseStat;


import java.io.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;


import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;


/**
 * OkuyamaFuse.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class OkuyamaFilesystem implements Filesystem3, XattrSupport {

    private static final Log log = LogFactory.getLog(OkuyamaFilesystem.class);

    public volatile static int blockSizeAssist = 2024;

    public volatile static int blockSize = 5632; // Blockサイズ

    
    public volatile static int writeBufferSize = 1024 * 1024 * 4 + 1024;

    public static final int maxSingleModeCacheSize = 200000;


    public static int storageType = -1;


    private FuseStatfs statfs;

    private String masterNodeStr = null;

    OkuyamaClientWrapper client = null;

    private Map openStatusMap = new Hashtable();

    private Map appendWriteDataBuf = new HashMap(10);
    private GroupingKeyMap writeBufFpMap = new GroupingKeyMap();

    private Object[] parallelDataAccessSync = new Object[100];

    public volatile static boolean jvmShutdownStatus = false;
    public static List allChildThreadList = null;


    public OkuyamaFilesystem(String okuyamaMasterNode, boolean singleMode) throws IOException {
        log.info("okuyama file system init start...");

        int files = 0;
        int dirs = 0;
        int blocks = 0;

        this.masterNodeStr = masterNodeStr;

        statfs = new FuseStatfs();
        statfs.blocks = Integer.MAX_VALUE;
        statfs.blockSize = blockSize;
        statfs.blocksFree = Integer.MAX_VALUE;
        statfs.files = files + dirs;
        statfs.filesFree = Integer.MAX_VALUE;
        statfs.namelen = 2048;
        try {
            for (int idx = 0; idx < 100; idx++) {
                this.parallelDataAccessSync[idx] = new Object();
            }
            client = new OkuyamaClientWrapper(singleMode);
        } catch (Exception e) {
            throw new IOException(e);
        }
        log.info("okuyama file system init end...");
   }



    public int chmod(String path, int mode) throws FuseException {
        log.info("chmod " + path + " " + mode);
        String[] pathInfo = null;
        StringBuilder newPathInfo = new StringBuilder();
        try {

            String pathInfoStr = client.getPathDetail(path.trim());
            if (pathInfoStr == null || pathInfoStr.trim().equals("")) return Errno.ENOENT;

            pathInfo = pathInfoStr.split("\t");

            pathInfo[7] = new Integer((new Integer(pathInfo[7]).intValue() & FuseStatConstants.TYPE_MASK) | (mode & FuseStatConstants.MODE_MASK)).toString();

            String sep = "";
            for (int i = 0; i < pathInfo.length; i++) {
                newPathInfo.append(sep);
                newPathInfo.append(pathInfo[i]);
                sep = "\t";
            }
            client.setPathDetail(path.trim(), newPathInfo.toString());
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            new FuseException(e).initErrno(FuseException.EACCES);
        }
        return 0;
    }

    public int chown(String path, int uid, int gid) throws FuseException {
        log.info("chown " + path + " " + uid + " " + gid);
        try {

            String pathInfoStr = client.getPathDetail(path.trim());
            if (pathInfoStr == null) return Errno.ENOENT;

            String[] pathInfo = pathInfoStr.split("\t");

            pathInfo[2] = new Integer(uid).toString(); // ユーザ変更
            pathInfo[3] = new Integer(gid).toString(); // グループ変更

            StringBuilder strBuf = new StringBuilder(100);

            String sep = "";
            for (int i = 0; i < pathInfo.length; i++) {

                strBuf.append(sep);
                strBuf.append(pathInfo[i]);
                sep = "\t";
            }

            client.setPathDetail(path.trim(), strBuf.toString());
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            new FuseException(e).initErrno(FuseException.EACCES);
        }
        return 0;
        //throw new FuseException("Read Only").initErrno(FuseException.EACCES);
    }


   public int getattr(String path, FuseGetattrSetter getattrSetter) throws FuseException {
        log.info("getattr " + path);
        String[] pathInfo = null;

        try {
            String[] setInfo = new String[11];


            if (path.trim().equals("/")) {


                setInfo[1] = new Integer(FuseFtypeConstants.TYPE_DIR | 0777).toString();
                pathInfo = new String[9];
                pathInfo[1] = "0";
                pathInfo[2] = "0";
                pathInfo[3] = "0";
                pathInfo[4] = "0";
                pathInfo[5] = "0";
                pathInfo[6] = "0";
                pathInfo[8] = "0";
            } else {
                String pathInfoStr = client.getPathDetail(path.trim());

                if (pathInfoStr == null || pathInfoStr.trim().equals("")) return Errno.ENOENT;
        
                pathInfo = pathInfoStr.split("\t");
                if (pathInfo[0].equals("file")) {
                    setInfo[1] = new Integer(FuseFtypeConstants.TYPE_FILE | new Integer(pathInfo[7]).intValue()).toString();
                } else if (pathInfo[0].equals("dir")) {
                    setInfo[1] = new Integer(FuseFtypeConstants.TYPE_DIR | new Integer(pathInfo[7]).intValue()).toString();
                } else {
                    throw new FuseException("No such entry").initErrno(FuseException.ENOENT);
                }
            }
        /*
            stat.nlink = 1;
            stat.uid = 0;
            stat.gid = 0;
            stat.size = 1000;
            stat.atime = 0;
            stat.blocks = 2;
        */
            setInfo[0] = new Integer(path.hashCode()).toString(); // inode

            setInfo[2] = pathInfo[1];
            //stat.nlink = Integer.parseInt(pathInfo[1]);

            setInfo[3] = pathInfo[2];
            ///stat.uid = Integer.parseInt(pathInfo[2]);

            setInfo[4] = pathInfo[3];
            //stat.gid = Integer.parseInt(pathInfo[3]);

            setInfo[5] = pathInfo[8]; // rdev

            setInfo[6] = pathInfo[4];
            //stat.size = Integer.parseInt(pathInfo[4]);

            long blockCnt = Long.parseLong(setInfo[6]) / 512;
            if (Long.parseLong(setInfo[6]) % 512 > 0) blockCnt++;
            //setInfo[7] = Long.parseLong(setInfo[6]) % 512;
            
            //stat.blocks = Integer.parseInt(pathInfo[6]);

            setInfo[8] = pathInfo[5];
            //stat.atime = Integer.parseInt(pathInfo[5]);

            setInfo[9] = pathInfo[5]; // mtime

            setInfo[10] = pathInfo[5]; // ctime


            getattrSetter.set(new Long(setInfo[0]).longValue(), 
                              new Integer(setInfo[1]).intValue(),
                              new Integer(setInfo[2]).intValue(),
                              new Integer(setInfo[3]).intValue(),
                              new Integer(setInfo[4]).intValue(),
                              new Integer(setInfo[5]).intValue(),
                              new Long(setInfo[6]).longValue(),
                              blockCnt,
                              new Integer(setInfo[8]).intValue(),
                              new Integer(setInfo[9]).intValue(),
                              new Integer(setInfo[10]).intValue());

        } catch (FuseException fe) {
            fe.printStackTrace();
            throw fe;
        } catch (Exception e) {
            e.printStackTrace();
            new FuseException(e).initErrno(FuseException.EACCES);
        }
        return 0;
   }

    public int getdir(String path, FuseDirFiller dirFiller) throws FuseException {
        log.info("getdir " + path);

        try {
            Map dirChildMap =  client.getDirChild(path.trim());

            if (dirChildMap == null) return Errno.ENOTDIR;

            int i = 0;
            Set entrySet = dirChildMap.entrySet();
            Iterator entryIte = entrySet.iterator(); 
            while(entryIte.hasNext()) {
                Map.Entry obj = (Map.Entry)entryIte.next();
    
                String name = (String)obj.getKey();
                String[] nameCnv = name.split("/");

                dirFiller.add(nameCnv[nameCnv.length - 1], 0L, ((String)obj.getValue()).equals("dir")? FuseFtype.TYPE_DIR : FuseFtype.TYPE_FILE);
                i++;
            }
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            throw new FuseException(e);
        }
        return 0;
    }

    public int link(String from, String to) throws FuseException {
        log.info("link " + from + " " + to);
        return Errno.EACCES;
    }

    public int mkdir(String path, int mode) throws FuseException {
        log.info("mkdir " + path + " " + mode);
        try {
            if(client.addDir(path.trim()) == false) return Errno.EEXIST;
            if(client.setDirAttribute(path.trim(), "dir") == false) return Errno.EEXIST;
            
            
            // setPathDetail
            String pathType = "dir";

            String pathInfoStr = "";
            pathInfoStr = pathInfoStr + pathType;
            pathInfoStr = pathInfoStr + "\t" + "1";
            pathInfoStr = pathInfoStr + "\t" + "0";
            pathInfoStr = pathInfoStr + "\t" + "0";
            pathInfoStr = pathInfoStr + "\t" + "0";
            pathInfoStr = pathInfoStr + "\t" + (System.currentTimeMillis() / 1000L);
            pathInfoStr = pathInfoStr + "\t" + "0";
            pathInfoStr = pathInfoStr + "\t" + mode;
            pathInfoStr = pathInfoStr + "\t" + "0"; //rdev
            pathInfoStr = pathInfoStr + "\t" + System.nanoTime(); // realKeyNodeNo

            if (!client.addPathDetail(path.trim(), pathInfoStr)) {
                return Errno.EEXIST;
            }
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            throw new FuseException(e);
        }
        return 0;
    }

    public int mknod(String path, int mode, int rdev) throws FuseException {
        log.info("mknod " + path + " " + mode + " " + rdev);
        // modeは8進数化して上3桁は作成するもののタイプを表す　http://sourceforge.net/apps/mediawiki/fuse/index.php?title=Stat
        // 下3桁はファイルパーミッション=>755とか644とか
        String modeStr = Integer.toOctalString(mode);
        String pathType = "";
        String fileBlockIdx = null;
        if (modeStr.indexOf("100") == 0) {

            // Regular File
            pathType = "file";
            fileBlockIdx = "-1";
        } else if (modeStr.indexOf("40") == 0) {

            // Directory
            pathType = "dir";
        } else {
            return Errno.EINVAL;
        }

        String pathInfoStr = "";
        pathInfoStr = pathInfoStr + pathType;
        pathInfoStr = pathInfoStr + "\t" + "1";
        pathInfoStr = pathInfoStr + "\t" + "0";
        pathInfoStr = pathInfoStr + "\t" + "0";
        pathInfoStr = pathInfoStr + "\t" + "0";
        pathInfoStr = pathInfoStr + "\t" + (System.currentTimeMillis() / 1000L);
        pathInfoStr = pathInfoStr + "\t" + "0";
        pathInfoStr = pathInfoStr + "\t" + mode;
        pathInfoStr = pathInfoStr + "\t" + rdev;
        pathInfoStr = pathInfoStr + "\t" + System.nanoTime(); // realKeyNodeNo
        if (fileBlockIdx != null) {
            pathInfoStr = pathInfoStr + "\t" + fileBlockIdx;
        }

        try {        
            if (!client.addPathDetail(path.trim(), pathInfoStr)) {
                return Errno.EEXIST;
            }
            if (!client.setDirAttribute(path.trim(), pathType)){
                return Errno.EEXIST;
            }
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            new FuseException(e);
        }
        return 0;
    }

    public int open(String path, int flags, FuseOpenSetter openSetter) throws FuseException {

        log.info("open " + path + " " + flags);
        long fileDp = System.nanoTime();
        try {
            String pathInfoStr = client.getPathDetail(path.trim());
            if (pathInfoStr == null || pathInfoStr.trim().equals("")) return Errno.ENOENT;
            Map openDt = new HashMap();
            openDt.put("filedp", fileDp); // openの状態を格納（Long型）(Fileディスクリプタ)
            openDt.put("pathInfoStr", pathInfoStr);
            openStatusMap.put(path.trim(), openDt);
            openSetter.setFh(openDt);
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            e.printStackTrace();
            new FuseException(e);
        }
        return 0;
    }

    public int rename(String from, String to) throws FuseException {
        log.info("rename " + from + " " + to);
        String[] pathInfo = null;
        StringBuilder newPathInfo = new StringBuilder();
        try {

            synchronized (this.parallelDataAccessSync[((from.hashCode() << 1) >>> 1) % 100]) {

                // 読み込むファイルがバッファリング書き込み中の場合一旦全てのバッファをflushする
                List bufferedDataFhList = writeBufFpMap.removeGroupingData(from);
                if (bufferedDataFhList != null) {
                    for (int idx = 0; idx < bufferedDataFhList.size(); idx++) {
                        Object bFh = bufferedDataFhList.get(idx);
                        this.fixNoCommitData(bFh);
                    }
                }

                String pathInfoStr = client.getPathDetail(from.trim());
                if (pathInfoStr == null || pathInfoStr.trim().equals("")) return Errno.ENOENT;

                pathInfo = pathInfoStr.split("\t");


                if (!client.addPathDetail(to.trim(), pathInfoStr)) {
                    return Errno.EEXIST;
                }
                if (!client.setDirAttribute(to.trim(), pathInfo[0])){
                    return Errno.EEXIST;
                }

                 if(client.removePathDetail(from.trim()) == false) return Errno.EIO;
                 if(client.removeAttribute(from.trim()) == false) return Errno.EIO;
            }
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            new FuseException(e).initErrno(FuseException.EACCES);
        }
        return 0;
    }

    public int rmdir(String path) throws FuseException {
        log.info("rmdir " + path);
        try {

            String pathInfoStr = client.getPathDetail(path.trim()); 
            if (pathInfoStr == null) return Errno.ENOTDIR;

            Map dirChildMap = client.getDirChild(path.trim());
            if (dirChildMap != null && dirChildMap.size() > 0) {
                return Errno.ENOTEMPTY;
            }

            if(client.removePathDetail(path.trim()) == false) return Errno.EBUSY;
            if(client.removeDir(path.trim()) == false) return Errno.EBUSY;
        } catch (Exception e) {
            new FuseException(e).initErrno(FuseException.EACCES);
        }
        return 0;
    }

    public int statfs(FuseStatfsSetter statfsSetter) throws FuseException {
        log.info("statfs " + statfsSetter);
        statfsSetter.set(
            blockSize,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            0,
            Integer.MAX_VALUE,
            2000
        );
        return 0;
    }

    public int symlink(String from, String to) throws FuseException {
        log.info("symlink " + from + " " + to);
        return Errno.EACCES;
    }

    public int truncate(String path, long size) throws FuseException {
       log.info("truncate " + path + " " + size);
       try {
            synchronized (this.parallelDataAccessSync[((path.hashCode() << 1) >>> 1) % 100]) {

                // 読み込むファイルがバッファリング書き込み中の場合一旦全てのバッファをflushする
                List bufferedDataFhList = writeBufFpMap.removeGroupingData(path);
                if (bufferedDataFhList != null) {
                    for (int idx = 0; idx < bufferedDataFhList.size(); idx++) {
                        Object bFh = bufferedDataFhList.get(idx);
                        this.fixNoCommitData(bFh);
                    }
                }

                String pathInfoStr = client.getPathDetail(path.trim()); 
                if (pathInfoStr == null) return Errno.ENOENT;
                String[] pathInfo = pathInfoStr.split("\t");


                if (new Long(pathInfo[4].trim()).longValue() > size) {

                    client.removeValue(path.trim(), size, (Long.parseLong(pathInfo[4]) - size), pathInfo[pathInfo.length - 2]);
                } else {

                    client.appendingNullData(path.trim(), size, pathInfo[pathInfo.length - 2]);
                }

                pathInfo[4] = new Long(size).toString();
                pathInfo[5] = new Long(System.currentTimeMillis() / 1000L).toString();
                int assistBlockSize = 0;
                if((size % blockSize) > 0) {
                    assistBlockSize = 1;
                }
                pathInfo[6] = new Long((size / blockSize) + assistBlockSize).toString();
                StringBuilder strBuf = new StringBuilder(64);
                strBuf.append(pathInfo[0]).append("\t").
                append(pathInfo[1]).append("\t").
                append(pathInfo[2]).append("\t").
                append(pathInfo[3]).append("\t").
                append(pathInfo[4]).append("\t").
                append(pathInfo[5]).append("\t").
                append(pathInfo[6]).append("\t").
                append(pathInfo[7]).append("\t").
                append(pathInfo[8]).append("\t").
                append(pathInfo[9]).append("\t").
                append("-1");
                
                if(!client.setPathDetail(path.trim(), strBuf.toString())) return Errno.EIO;
            }
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            throw new FuseException(e).initErrno(FuseException.EACCES);
        }
        return 0;
    }

    public int unlink(String path) throws FuseException {
        log.info("unlink " + path);

        try {
            synchronized (this.parallelDataAccessSync[((path.hashCode() << 1) >>> 1) % 100]) {

                // 読み込むファイルがバッファリング書き込み中の場合一旦全てのバッファをflushする
                List bufferedDataFhList = writeBufFpMap.removeGroupingData(path);
                if (bufferedDataFhList != null) {
                    for (int idx = 0; idx < bufferedDataFhList.size(); idx++) {
                        Object bFh = bufferedDataFhList.get(idx);
                        this.fixNoCommitData(bFh);
                    }
                }

                String pathInfoStr = client.getPathDetail(path.trim()); 
                if (pathInfoStr == null) return Errno.ENOENT;
                String[] pathInfo = pathInfoStr.split("\t");

                if(client.removePathDetail(path.trim()) == false) return Errno.EIO;
                if(client.removeAttribute(path.trim()) == false) return Errno.EIO;
                client.deleteValue(path.trim(), pathInfo[pathInfo.length - 2]);
            }
        }  catch(Exception e) {
            throw new FuseException(e).initErrno(FuseException.EIO);
        }
        return 0;
    }

    public int utime(String path, int atime, int mtime) throws FuseException {
        log.info("utime " + path + " " + atime + " " + mtime);
        return 0;
    }

   public int readlink(String path, CharBuffer link) throws FuseException {
        log.info("readlink " + path);
        link.append(path);
        return 0;
   }

   public int write(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException {
        //log.info("write  path:" + path + " offset:" + offset + " isWritepage:" + isWritepage + " buf.limit:" + buf.limit());
        try {

            // メモリモードの場合はバッファリングするだけ無駄なので即時書き出し
            if (OkuyamaFilesystem.storageType == 1) return realWrite(path, fh, isWritepage, buf, offset);

            if (fh == null) return Errno.EBADE;
            //log.info("write point=" + offset);

            synchronized (this.parallelDataAccessSync[((path.hashCode() << 1) >>> 1) % 100]) {

                if (appendWriteDataBuf.containsKey(fh)) {

                    Map appendData = (Map)appendWriteDataBuf.remove(fh);
                    String bPath = (String)appendData.get("path");
                    Object bFh = (Object)appendData.get("fh");
                    boolean bIsWritepage = ((Boolean)appendData.get("isWritepage")).booleanValue();
                    ByteArrayOutputStream bBuf = (ByteArrayOutputStream)appendData.get("buf");
                    long bOffset = ((Long)appendData.get("offset")).longValue();

                    if ((bOffset + bBuf.size()) == offset) {

                        byte[] tmpBuf = new byte[buf.limit()];
                        buf.get(tmpBuf);
                        bBuf.write(tmpBuf);

                        int count = ((Integer)appendData.get("count")).intValue();

                        // 既に規定回数のバッファリングを行ったか、規定バイト数を超えた場合に強制的に書き出し
                        if (count > OkuyamaFilesystem.blockSizeAssist || bBuf.size() >= writeBufferSize) {

                            // まとめて書き出す
                            return this.realWrite(bPath, bFh, bIsWritepage, bBuf, bOffset);
                        } else {

                            appendData.put("buf", bBuf);
                            count++;
                            appendData.put("count", count);
                            this.appendWriteDataBuf.put(fh, appendData);
                            this.writeBufFpMap.addGroupingData(path, fh);
                            return 0;
                        }
                    } else {

                        // offsetが違うので、それぞれ個別で書き出す
                        int realWriteRet = this.realWrite(bPath, bFh, bIsWritepage, bBuf, bOffset);
                        if(realWriteRet == 0) {

                            return this.realWrite(path, fh, isWritepage, buf, offset);
                        } else {
                            return realWriteRet;
                        }
                    }
                } else {

                    Map appendData = new HashMap();
                    appendData.put("path", path);
                    appendData.put("fh", fh);
                    appendData.put("isWritepage", isWritepage);
                    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024*20);
                    byte[] tmpByte = new byte[buf.limit()];
                    buf.get(tmpByte);
                    baos.write(tmpByte);
                    appendData.put("buf", baos);
                    appendData.put("offset", offset);
                    appendData.put("count", 1);
                    this.appendWriteDataBuf.put(fh, appendData);
                    this.writeBufFpMap.addGroupingData(path, fh);
                    return 0;
                }
            }
        } catch (Exception e) {
            throw new FuseException(e);
        }
    }

    public int realWrite(String path, Object fh, boolean isWritepage, ByteBuffer buf, long offset) throws FuseException {
        byte[] writeData = new byte[buf.limit()];
        buf.get(writeData);
        return realWrite(path, fh, isWritepage, writeData, offset);
    }


    public int realWrite(String path, Object fh, boolean isWritepage,  ByteArrayOutputStream buf, long offset) throws FuseException {
        return realWrite(path, fh, isWritepage, buf.toByteArray(), offset);
    }

    public int realWrite(String path, Object fh, boolean isWritepage,  byte[] writeData, long offset) throws FuseException {

        //log.info("realWrite  path:" + path + " offset:" + offset + " isWritepage:" + isWritepage + " buf.limit:" + writeData.length);
        if (fh == null) return Errno.EBADE;
        //long start = System.nanoTime();
        try {

            String pathTrimStr = path.trim();
            String pathInfoStr = (String)client.getPathDetail(pathTrimStr);

            if (pathInfoStr == null) return Errno.ENOENT;

            String[] pathInfo = pathInfoStr.split("\t");

            //long end2 = System.nanoTime();
            //System.out.println("Time2=" + (end2 - start));

            long writeLastBlockIdx = -1L;
            if (writeData == null || writeData.length < 1) {

                log.error("write data nothing = realWrite  path:" + path + " offset:" + offset + " isWritepage:" + isWritepage + " buf.limit:" + writeData.length);
            } else {

                int idx = writeData.length;
                writeLastBlockIdx = client.writeValue(pathTrimStr, offset, writeData, writeData.length, pathInfo[pathInfo.length - 2], new Long(pathInfo[10]).longValue());
            }
            //long end3 = System.nanoTime();
            //System.out.println("Time3=" + (end3 - end2));

            if (Long.parseLong(pathInfo[4]) < 1)  {

                pathInfo[4] = new Long(writeData.length).toString();
            } else if (Long.parseLong(pathInfo[4]) < new Long(offset + new Long(writeData.length).longValue()).longValue()) {

                pathInfo[4] = new Long(offset + new Long(writeData.length).longValue()).toString();
            }

            pathInfo[5] = new Long(System.currentTimeMillis() / 1000L).toString();
            int assistBlockSize = 0;
            if(((Long.parseLong(pathInfo[4]) + writeData.length) % blockSize) > 0) {
                assistBlockSize = 1;
            }
            pathInfo[6] = new Long(((Long.parseLong(pathInfo[4]) + writeData.length) / blockSize) + assistBlockSize).toString();
            StringBuilder strBuf = new StringBuilder(64);
            strBuf.append(pathInfo[0]).append("\t").
            append(pathInfo[1]).append("\t").
            append(pathInfo[2]).append("\t").
            append(pathInfo[3]).append("\t").
            append(pathInfo[4]).append("\t").
            append(pathInfo[5]).append("\t").
            append(pathInfo[6]).append("\t").
            append(pathInfo[7]).append("\t").
            append(pathInfo[8]).append("\t").
            append(pathInfo[9]).append("\t");
            if (new Long(pathInfo[10]).longValue() < writeLastBlockIdx) {
                strBuf.append(writeLastBlockIdx);
            } else {
                strBuf.append(pathInfo[10]);
            }
            client.setPathDetail(pathTrimStr, strBuf.toString());
            //long end4 = System.nanoTime();
            //System.out.println("Time4=" + (end4 - end3));
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            new FuseException(e);
        }
        //long end = System.nanoTime();
        //System.out.println("Time=" + (end - start));
        return 0;
    }

    public int read(String path, Object fh, ByteBuffer buf, long offset) throws FuseException {
        /*long start1 = 0L;
        long start2 = 0L;
        long start3 = 0L;
        long start4 = 0L;
        long start5 = 0L;

        long end2 = 0L;
        long end3 = 0L;
        long end4 = 0L;
        long end5 = 0L;
        */
        //start1 = System.nanoTime();
        //start2 = System.nanoTime();

        //log.info("read:" + path + " offset:" + offset + " buf.limit:" + buf.limit());
        if (fh == null) return Errno.EBADE;
        //end2 = System.nanoTime();
        try {

            //start3 = System.nanoTime();
            String trimToPath = path.trim();
            synchronized (this.parallelDataAccessSync[((path.hashCode() << 1) >>> 1) % 100]) {

                // 読み込むファイルがバッファリング書き込み中の場合一旦全てのバッファをflushする
                List bufferedDataFhList = writeBufFpMap.removeGroupingData(path);
                if (bufferedDataFhList != null) {
                    for (int idx = 0; idx < bufferedDataFhList.size(); idx++) {
                        Object bFh = bufferedDataFhList.get(idx);
                        this.fixNoCommitData(bFh);
                    }
                }

                String pathInfoStr = (String)client.getPathDetail(trimToPath);
                //end3 = System.nanoTime();

                //start4 = System.nanoTime();

                String[] pathInfo = pathInfoStr.split("\t");
                //end4 = System.nanoTime();

                //start5 = System.nanoTime();

                byte[] readData = client.readValue(trimToPath, offset, buf.limit(), pathInfo[pathInfo.length - 2]);

                //end5 = System.nanoTime();
                
                if (readData != null && readData.length > 0) {

                    buf.put(readData);
                } else {

                    log.info("read data nothing read=" + "read:" + path + " offset:" + offset + " buf.limit:" + buf.limit());
                }
            }
        } catch (FuseException fe) {
            throw fe;
        } catch (Exception e) {
            new FuseException(e);
        }

        //long end1 = System.nanoTime();
        //System.out.println("1=" + (end1 - start1) + " 2=" + (end2 - start2) + " 3=" + (end3 - start3) + " 4=" + (end4 - start4) + " 5=" + (end5 - start5));
        return 0;
    }

    public int release(String path, Object fh, int flags) throws FuseException {
        log.info("release " + path + " " + fh +  " " + flags);
        synchronized (this.parallelDataAccessSync[((path.hashCode() << 1) >>> 1) % 100]) {

            List bufferedDataFhList = writeBufFpMap.removeGroupingData(path);

            if (bufferedDataFhList != null) {
                for (int idx = 0; idx < bufferedDataFhList.size(); idx++) {
                    Object bFh = bufferedDataFhList.get(idx);
                    this.fixNoCommitData(bFh);
                }
            }

            openStatusMap.remove(path.trim());
            this.fixNoCommitData(fh);
        }

        return 0;
    }

    public int flush(String path, Object fh) throws FuseException {
        synchronized (this.parallelDataAccessSync[((path.hashCode() << 1) >>> 1) % 100]) {
            log.info("flush " + path + " " + fh);
            List bufferedDataFhList = writeBufFpMap.removeGroupingData(path);

            if (bufferedDataFhList != null) {
                for (int idx = 0; idx < bufferedDataFhList.size(); idx++) {
                    Object bFh = bufferedDataFhList.get(idx);
                    this.fixNoCommitData(bFh);
                }
            }

            this.fixNoCommitData(fh);
        }
        return  0;
   }


    public int fsync(String path, Object fh, boolean isDatasync) throws FuseException {
        synchronized (this.parallelDataAccessSync[((path.hashCode() << 1) >>> 1) % 100]) {
            log.info("fsync " + path + " " + fh + " " + isDatasync);
            List bufferedDataFhList = writeBufFpMap.removeGroupingData(path);

            if (bufferedDataFhList != null) {
                for (int idx = 0; idx < bufferedDataFhList.size(); idx++) {
                    Object bFh = bufferedDataFhList.get(idx);
                    this.fixNoCommitData(bFh);
                }
            }

            this.fixNoCommitData(fh);
        }
        return 0;
   }



    private int fixNoCommitData(Object fh) throws FuseException {

        if (appendWriteDataBuf.containsKey(fh)) {
            Map appendData = (Map)appendWriteDataBuf.remove(fh);
            String bPath = (String)appendData.get("path");
            Object bFh = (Object)appendData.get("fh");
            boolean bIsWritepage = ((Boolean)appendData.get("isWritepage")).booleanValue();
            ByteArrayOutputStream bBuf = (ByteArrayOutputStream)appendData.get("buf");
            long bOffset = ((Long)appendData.get("offset")).longValue();

            int realWriteRet = this.realWrite(bPath, bFh, bIsWritepage, bBuf, bOffset);
            return realWriteRet;
        }
        return 0;
    }


   public int getxattr(String path, String name, ByteBuffer dst) throws FuseException, BufferOverflowException
   {

      return 0;
   }

   public int getxattrsize(String path, String name, FuseSizeSetter sizeSetter) throws FuseException
   {

      return 0;
   }

   public int listxattr(String path, XattrLister lister) throws FuseException
   {

      return 0;
   }

   public int removexattr(String path, String name) throws FuseException
   {
      return 0;
   }

   public int setxattr(String path, String name, ByteBuffer value, int flags) throws FuseException
   {
      return 0;
   }


    public static void main(String[] args) {

        String fuseArgs[] = new String[args.length - 1];
        System.arraycopy(args, 0, fuseArgs, 0, fuseArgs.length);
     
        log.info("entering");
     
        try {
            String okuyamaStr = args[args.length - 1];
            String[] masterNodeInfos = null;
            if (okuyamaStr.indexOf(",") != -1) {
                masterNodeInfos = okuyamaStr.split(",");
            } else {
                masterNodeInfos = (okuyamaStr + "," + okuyamaStr).split(",");
            }
            // 1=Mmoery
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
        }
        catch (Exception e)
        {
           e.printStackTrace();
        }
        finally
        {
           log.info("exiting");
        }
    }

}


package fuse.okuyamafs;

import java.util.*;
import java.nio.*;
import java.io.*;

/**
 * OkuyamaFuse.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
public class OkuyamaClientWrapper {


    static IFsMap dataMap = CoreMapFactory.createDataMap();

    static IFsMap infoMap = CoreMapFactory.createInfoMap();

    static IFsMap dirMap = CoreMapFactory.createDirMap();

    static byte[] dummyBuf = new byte[OkuyamaFilesystem.blockSize];

    volatile static boolean singleMode = false;

    
    static Map infoCache = new ExpireCacheMap(50000, 60*1000);


    static {
        for (int dummyIdx = 0; dummyIdx < OkuyamaFilesystem.blockSize; dummyIdx++) {
            dummyBuf[dummyIdx] = 0;
        }
    }

    // singlerModeは1つのデータを複数の異なるマウントサーバからアクセスされる際はfalseとする。一つのサーバの際はtrueとする
    public OkuyamaClientWrapper(boolean singleMode) throws Exception {
        addDir("/", false);
        OkuyamaClientWrapper.singleMode = singleMode;
    }

    // ディレクトリ情報をadd(新規のみ)する
    public boolean addDir(String path) throws Exception {
        return addDir(path, true);
    }

    // ディレクトリ情報をadd(新規のみ)する
    public boolean addDir(String path, boolean checkExist) throws Exception {
        if (checkExist == false) {

            dirMap.putNewMap(path, new LinkedHashMap());
            return true;
        } else {

            return dirMap.putNewMap(path, new LinkedHashMap());
        }
    }

    // ディレクトリ情報をset(上書きも含む)する
    // attributeNameは"/var/tmp/a.txt"まで
    // type=file(File), type=dir(Dir)
    public boolean setDirAttribute(String attributeName, String type)  throws Exception {
        
        String dirName = "";
        String[] attributeNameCnv = attributeName.trim().split("/");

        if (attributeNameCnv.length == 1) {
            dirName = "/";
        } else {
            String sep = "/";
            int cnt = 0;
            for (int i = 0; i < (attributeNameCnv.length - 1); i++) {
                if (attributeNameCnv[i].trim().equals("")) continue;
                dirName =  dirName + sep + attributeNameCnv[i];
                cnt++;
            }
            if (cnt == 0) dirName = "/";
        }

        if (dirMap.containsKey(dirName) == false) return false;
        Map dirDt = dirMap.getMap(dirName);

        dirDt.put(attributeName, type);

        dirMap.putMap(dirName, dirDt);
        return true;
    }


    public boolean removeDir(String path) throws Exception {

        if (dirMap.containsKey(path) == false) return false;

        Map dirDt = dirMap.getMap(path);

        if (dirDt != null && dirDt.size() > 0) return false;

        String[] parentDirList = path.split("/");
        if (parentDirList.length > 0) {

            int dirCnt = 0;
            String parentDirStr = "/";
            String sep = "";
            for (int idx = 0; idx < (parentDirList.length - 1); idx++) {
                if (!parentDirList[idx].trim().equals("")) {
                    parentDirStr = parentDirStr + sep + parentDirList[idx].trim();
                    dirCnt++;
                    sep = "/";
                }
            }

            Map parentDirDt = (Map)dirMap.getMap(parentDirStr);
            parentDirDt.remove(path);
            dirMap.putMap(parentDirStr, parentDirDt);
        }

        dirMap.remove(path);
        return true;
    }


    public boolean removeAttribute(String attributeName) throws Exception {
        String dirName = "";
        String[] attributeNameCnv = attributeName.trim().split("/");

        if (attributeNameCnv.length == 1) {
            dirName = "/";
        } else {
            String sep = "/";
            for (int i = 0; i < (attributeNameCnv.length - 1); i++) {
                if (attributeNameCnv[i].trim().equals("")) continue;
                dirName =  dirName + sep + attributeNameCnv[i];
            }
            if (attributeName.indexOf("/") == 0) {
                if (dirName.trim().equals("")) dirName = "/";
            }
        }
        if (dirMap.containsKey(dirName) == false) return false;
        Map dirDt = dirMap.getMap(dirName);

        dirDt.remove(attributeName);
        dirMap.putMap(dirName, dirDt);
        return true;
    }

    // Mapの要素はKey=ディレクトリorファイル名 Value=ファイルorDirを表す値(1 or 2)のString配列 
    // 指定したパス直下の情報を返す
    public Map getDirChild(String path) throws Exception {
        Map dirDt = dirMap.getMap(path);

        return dirDt;
    }
   
    public String getPathDetail(String key) throws Exception {
        String retStr = null;

        if (singleMode == true) retStr = (String)infoCache.get(key);

        if (retStr != null) return retStr;
        // Cacheにない
        retStr = infoMap.getString(key);

        if (retStr != null) infoCache.put(key, retStr);
        return retStr;
    }



    public byte[] readValue(String key, long start, int offset, String realKeyNodeNo) throws Exception {

        //List dataReadKeyList = new ArrayList();
        String[] dataReadKeyList = null;
        boolean allDataNull = true;


        long readStartPoint = (start / OkuyamaFilesystem.blockSize);
        long assistPoint = 0L;
        if (((start + offset) % OkuyamaFilesystem.blockSize) > 0) assistPoint = 1;
        long readEndPoint = ((start + offset) / OkuyamaFilesystem.blockSize);
        readEndPoint = readEndPoint + assistPoint;

        byte[] replaceDataBuf = null;
        int bufReadPoint = new Long(start - ((start / OkuyamaFilesystem.blockSize) * OkuyamaFilesystem.blockSize)).intValue();


        if (readStartPoint == readEndPoint) {

            dataReadKeyList = new String[1];
            //dataReadKeyList.add(realKeyNodeNo + "\t" + readStartPoint);
            dataReadKeyList[0] = realKeyNodeNo + "\t" + readStartPoint;
        } else {

            dataReadKeyList = new String[(int)(readEndPoint - readStartPoint)];
            int idx = 0;
            for (long i = readStartPoint; i < readEndPoint; i++) {
                //dataReadKeyList.add(realKeyNodeNo + "\t" + i);
                dataReadKeyList[idx] = realKeyNodeNo + "\t" + i;
                idx++;
            }   
        }
        int t = 0;
        //int dataReadKeyListSize = dataReadKeyList.size();
        int dataReadKeyListSize = dataReadKeyList.length;
        replaceDataBuf = new byte[OkuyamaFilesystem.blockSize * dataReadKeyListSize];


        if (CoreMapFactory.factoryType == 2) {

            // okuyamaの場合事前にgetMultiByteでデータを取得しておく
            //Object[] keyList = new Object[dataReadKeyListSize];
            /*for (int i = 0; i < dataReadKeyListSize; i++) {
                keyList[i] = (String)dataReadKeyList.get(i);
            }*/
            if (dataReadKeyListSize > 1) {
                //Map multiReadRet = this.getMultiValue(keyList);
                Map multiReadRet = this.getMultiValue(dataReadKeyList);

                if (multiReadRet != null) {
                    for (int i = 0; i < dataReadKeyListSize; i++) {
                        byte[] readData = (byte[])multiReadRet.get(dataReadKeyList[i]);
                        if (readData == null) break;

                        allDataNull = false;
                        System.arraycopy(readData, 0, replaceDataBuf, t, readData.length);
                        t = t + readData.length;
                    }
                }
            } else {
                for (int i = 0; i < dataReadKeyListSize; i++) {
                    byte[] readData = getValue(dataReadKeyList[i]);
                    if (readData == null) break;

                    allDataNull = false;
                    System.arraycopy(readData, 0, replaceDataBuf, t, readData.length);
                    t = t + readData.length;
                }
            }
        } else {

            for (int i = 0; i < dataReadKeyListSize; i++) {
                byte[] readData = getValue(dataReadKeyList[i]);
                if (readData == null) break;

                allDataNull = false;
                System.arraycopy(readData, 0, replaceDataBuf, t, readData.length);
                t = t + readData.length;
            }
        }

        // 指定データ内で一件も取れなかった。
        if (allDataNull) {

            for (int i = 0; i < dataReadKeyListSize; i++) {
            }
            return null;
        }
        int readDataSize = replaceDataBuf.length;
        byte[] retData = null;
        int realWritePoint = 0;
        //byte[] readDataBuf = new byte[readDataSize];

        for (int startPoint = bufReadPoint; startPoint < (bufReadPoint + offset); startPoint++) {
            if (replaceDataBuf.length <=  startPoint) break;
            realWritePoint++;
        }

        if (replaceDataBuf.length <= (bufReadPoint + offset)) {

            retData = new byte[realWritePoint];
            System.arraycopy(replaceDataBuf, bufReadPoint, retData, 0, realWritePoint);
        } else if (replaceDataBuf.length > (bufReadPoint + offset)) {

            retData = new byte[realWritePoint];
            System.arraycopy(replaceDataBuf, bufReadPoint, retData, 0, realWritePoint);
        }
        return retData;
    }

    private byte[] getValue(String key) throws Exception {

        byte[] retData = dataMap.getBytes(key);
        return retData;

    }

    private Map getMultiValue(Object[] keyList) throws Exception {
        Map retData = ((OkuyamaFsMap)dataMap).getMultiBytes(keyList);
        return retData;
    }
    

    public long writeValue(String key, long start, byte[] writeData, int limit, String realKeyNodeNo, long lastBlockIdx) throws Exception {



        long retBlockIdx = 0L;

        int realStartPoint = new Long(((start % OkuyamaFilesystem.blockSize))).intValue();
        int len = writeData.length;

        //List dataReadKeyList = new ArrayList(16);
        String[] dataReadKeyList = null;
        long writeStartPoint = (start / OkuyamaFilesystem.blockSize);

        int assistPoint = 0;
        if (((start + len) % OkuyamaFilesystem.blockSize) > 0) assistPoint = 1;
        long writeEndPoint = ((start + len) / OkuyamaFilesystem.blockSize);
        writeEndPoint = writeEndPoint + assistPoint;

        boolean fullFirtstBlock = false;
        // 現在の該当データで書き込み済みのブロックの最大Indexとこれから書きこもうとしているブロックのはじめを比べて、
        // 書きこもうとしているブロックが現時点での最大ブロックよりも大きければ新規ばかりの書き込みと判断する
        if (lastBlockIdx < writeStartPoint) fullFirtstBlock = true;

        if (writeStartPoint == writeEndPoint) {

            dataReadKeyList = new String[1];
            dataReadKeyList[0] = realKeyNodeNo + "\t" + writeStartPoint;
            //dataReadKeyList.add(realKeyNodeNo + "\t" + writeStartPoint);
        } else {

            dataReadKeyList = new String[(int)(writeEndPoint - writeStartPoint)];
            int assistIdx = 0;
            for (long i = writeStartPoint; i < writeEndPoint; i++) {
                dataReadKeyList[assistIdx] = realKeyNodeNo + "\t" + i;
                assistIdx++;
                //dataReadKeyList.add(realKeyNodeNo + "\t" + i);
            }
        }

        int dataReadKeyListSize = dataReadKeyList.length;

        byte[] replaceDataBuf = new byte[OkuyamaFilesystem.blockSize * dataReadKeyListSize];
        int copyStart = 0;

        if (lastBlockIdx == -1L || fullFirtstBlock == true) {

            for (int idx = 0; idx < dataReadKeyListSize; idx++) {

                System.arraycopy(OkuyamaClientWrapper.dummyBuf, 0, replaceDataBuf, copyStart, OkuyamaClientWrapper.dummyBuf.length);
                copyStart = copyStart + OkuyamaClientWrapper.dummyBuf.length;

            }

        } else {

            boolean firstDataExists = false;
            byte[] lastBlock = null;
            for (int idx = 0; idx < dataReadKeyListSize; idx++) {

                byte[] replaceData = null;
                // 最初の1レコード、最後の1レコードのみ取得する。これは途中のデータは新しいデータに絶対に上書きされるため、
                // 現行データが残る可能性があるデータは最後の1データのみであるためである。
                if (CoreMapFactory.factoryType == 1 || CoreMapFactory.factoryType == 2) {
                    if (idx == 0) {
                        Object[] keys = null;
                        if (dataReadKeyListSize > 1) {
                            keys = new Object[2];
                            keys[0] = dataReadKeyList[idx];
                            keys[1] = dataReadKeyList[dataReadKeyListSize - 1];
                            Map mRet = getMultiValue(keys);
                            //replaceData = getValue(dataReadKeyList[idx]);
                            byte[] firstData = (byte[])mRet.get((String)keys[0]);
                            lastBlock = (byte[])mRet.get((String)keys[1]);
                            replaceData = firstData;
                        } else {
                            replaceData = getValue(dataReadKeyList[idx]);
                        }


                        if (replaceData != null) firstDataExists = true;
                    } else if ((idx + 1) == dataReadKeyListSize) {
                        if (firstDataExists == true) {
                            replaceData = lastBlock;
                            //replaceData = getValue(dataReadKeyList[idx]);
                        }
                    }
                }


                if (replaceData != null) {

                    System.arraycopy(replaceData, 0, replaceDataBuf, copyStart, replaceData.length);
                    copyStart = copyStart + replaceData.length;
                } else {

                    System.arraycopy(OkuyamaClientWrapper.dummyBuf, 0, replaceDataBuf, copyStart, OkuyamaClientWrapper.dummyBuf.length);
                    copyStart = copyStart + OkuyamaClientWrapper.dummyBuf.length;
                }
            }

        }



        byte[] replaceAllData = replaceDataBuf;

        replaceDataBuf = null;
        if (copyStart == 0) {
            replaceAllData = new byte[len];
        }

        System.arraycopy(writeData, 0, replaceAllData, realStartPoint, len);

        String lastSetKey = null;



        if (CoreMapFactory.factoryType == 1) {

            byte[] blockDataBuf = new byte[OkuyamaFilesystem.blockSize];

            for (int idx = 0; idx < dataReadKeyListSize; idx++) {


                int stopPointIdx = 0;
                for (int t = idx * OkuyamaFilesystem.blockSize; t < replaceAllData.length; t++) {

                    blockDataBuf[stopPointIdx] = replaceAllData[t];
                    stopPointIdx++;
                    if (OkuyamaFilesystem.blockSize <= stopPointIdx) break;
                }

                byte[] setBytes = Arrays.copyOf(blockDataBuf, stopPointIdx);


                setValue(dataReadKeyList[idx], setBytes);
                lastSetKey = dataReadKeyList[idx];
            }
        } else {
            byte[] blockDataBuf = new byte[OkuyamaFilesystem.blockSize];

            Object[] putDataList = new Object[dataReadKeyListSize];
            for (int idx = 0; idx < dataReadKeyListSize; idx++) {


                int stopPointIdx = 0;
                for (int t = idx * OkuyamaFilesystem.blockSize; t < replaceAllData.length; t++) {

                    blockDataBuf[stopPointIdx] = replaceAllData[t];
                    stopPointIdx++;
                    if (OkuyamaFilesystem.blockSize <= stopPointIdx) break;
                }

                byte[] setBytes = Arrays.copyOf(blockDataBuf, stopPointIdx);

                Object[] putDataReq = new Object[2];
                putDataReq[0] = dataReadKeyList[idx];
                putDataReq[1] = setBytes;
                putDataList[idx] = putDataReq;

                lastSetKey = dataReadKeyList[idx];
            }

            ((OkuyamaFsMap)dataMap).putMultiBytes(putDataList);
        }

        //int tabPoint = lastSetKey.indexOf("\t");
        String retBlockIdxStr = lastSetKey.substring(lastSetKey.indexOf("\t") + 1);
        retBlockIdx = Long.parseLong(retBlockIdxStr);

        return retBlockIdx;
    }


    private boolean setValue(String key, byte[] value) throws Exception {
        dataMap.putBytes(key, value);
        return true;
    }



    public boolean removeValue(String key, long size, long removeSize, String realKeyNodeNo) throws Exception {

        if (size == 0L) {
            deleteValue(key, realKeyNodeNo);
            return true;
        }

        List dataReadKeyList = new ArrayList();
        Map dataBuf = new LinkedHashMap(8192);
        List dataList = new ArrayList(8192);
        
        int noRemoveStartPoint = new Long(size % OkuyamaFilesystem.blockSize).intValue();
        long readStartPoint = (size / OkuyamaFilesystem.blockSize);
        long assistPoint = 0;
        if (((size + removeSize) % OkuyamaFilesystem.blockSize) > 0) assistPoint = 1;
        long readEndPoint = ((size + removeSize) / OkuyamaFilesystem.blockSize);
        readEndPoint = readEndPoint + assistPoint;

        if (readStartPoint == readEndPoint) {
            dataReadKeyList.add(realKeyNodeNo + "\t" + readStartPoint);
        } else {
        
            for (long i = readStartPoint; i < readEndPoint; i++) {
                dataReadKeyList.add(realKeyNodeNo + "\t" + i);
            }
        }


        int dataReadKeyListSize = dataReadKeyList.size();
        if (dataReadKeyListSize > 1) {

            for (int idx = 1; idx < dataReadKeyListSize; idx++) {

                if (removeValue((String)dataReadKeyList.get(idx)) == false) break;
            }

        } else if (dataReadKeyListSize == 0) {

            return true;
        }

        byte[] readData = getValue((String)dataReadKeyList.get(0));
        for (int i = noRemoveStartPoint; i < OkuyamaFilesystem.blockSize; i++) {
            readData[i] = new Byte((byte)0).byteValue();
        }
        setValue((String)dataReadKeyList.get(0), readData);
        return true;
    }


    public boolean appendingNullData(String key, long size, String realKeyNodeNo) throws Exception {
        List dataReadKeyList = new ArrayList();
        Map dataBuf = new LinkedHashMap(8192);
        List dataList = new ArrayList(8192);
        long readStartPoint = 0;
        long readEndPoint = (size / OkuyamaFilesystem.blockSize);
        long assistPoint = 0L;
        if ((size % OkuyamaFilesystem.blockSize) > 0) readEndPoint = readEndPoint + 1;

        if (readStartPoint == readEndPoint) {
            dataReadKeyList.add(realKeyNodeNo + "\t" + readStartPoint);
        } else {
        
            for (long i = readStartPoint; i < readEndPoint; i++) {
                dataReadKeyList.add(realKeyNodeNo + "\t" + i);
            }   
        }
        int t = 0;
        int dataReadKeyListSize = dataReadKeyList.size();
        for (int i = 0; i < dataReadKeyListSize; i++) {

            ByteArrayOutputStream replaceDataBuf = new ByteArrayOutputStream(OkuyamaFilesystem.blockSize);
            byte[] setBytes = null;

            if (i == 0) {

                byte[] readData = getValue((String)dataReadKeyList.get(i));

                if (readData == null) {

                    for (int dummyIdx = 0; dummyIdx < OkuyamaFilesystem.blockSize; dummyIdx++) {
                        replaceDataBuf.write(0);
                    }
                    setBytes = replaceDataBuf.toByteArray();
                } else {
                    replaceDataBuf.write(readData);
                    for (int dummyIdx = readData.length; dummyIdx < OkuyamaFilesystem.blockSize; dummyIdx++) {
                        replaceDataBuf.write(0);
                    }
                    setBytes = replaceDataBuf.toByteArray();
                }
            } else if ((i+1) != dataReadKeyListSize) {
                for (int dummyIdx = 0; dummyIdx < OkuyamaFilesystem.blockSize; dummyIdx++) {
                    replaceDataBuf.write(0);
                }
                setBytes = replaceDataBuf.toByteArray();
            } else {
                byte[] readData = getValue((String)dataReadKeyList.get(i));

                if (readData == null) {

                    for (int dummyIdx = 0; dummyIdx < OkuyamaFilesystem.blockSize; dummyIdx++) {
                        replaceDataBuf.write(0);
                    }
                    setBytes = replaceDataBuf.toByteArray();
                } else {
                    replaceDataBuf.write(readData);
                    for (int dummyIdx = readData.length; dummyIdx < OkuyamaFilesystem.blockSize; dummyIdx++) {
                        replaceDataBuf.write(0);
                    }
                    setBytes = replaceDataBuf.toByteArray();
                }
            }

            setValue((String)dataReadKeyList.get(i), setBytes);
        }

        return true;
    }


   public boolean deleteValue(String key, String realKeyNodeNo) throws Exception {

        if (CoreMapFactory.factoryType != 2) {
            for (long idx = 0; idx < Long.MAX_VALUE; idx++) {
                if(removeValue(realKeyNodeNo + "\t" + idx) == false) break;
            }
        } else {

            for (long idx = 0; idx < Long.MAX_VALUE; idx++) {

                String[] keys = new String[50];
                for (int i = 0; i < 50; idx++) {
                    keys[i] = realKeyNodeNo + "\t" + idx;
                    i++;
                }

                if(multiRemoveValue(keys) == false) break;
            }
        }
        return true;
    }

    private boolean removeValue(String key) throws Exception {
        boolean ret = dataMap.containsKey(key);
        dataMap.remove(key);
        return ret;
    }

    private boolean multiRemoveValue(String[] keys) throws Exception {

        return ((OkuyamaFsMap)dataMap).removeMulti(keys);
    }


    public boolean setPathDetail(String key, String value) throws Exception {
        infoMap.putString(key, value);
        if (singleMode == true) infoCache.put(key, value);
        return true;
    }


    public boolean addPathDetail(String key, String value) throws Exception {
        if (infoMap.containsKey(key) == true) return false;
        infoMap.putString(key, value);
        if (singleMode == true) infoCache.put(key, value);
        return true;
    }

    public boolean removePathDetail(String key) throws Exception {

        if (singleMode == true) infoCache.remove(key);
        if (infoMap.containsKey(key) != true) return false;
        infoMap.remove(key);

        return true;
    }


    public Map getDataMetaInfo(String key) throws Exception {
        Map meta = new HashMap();
        meta.put("pathdetail", infoMap.getString(key));

        String attributeName = key;
        String dirName = "";
        String[] attributeNameCnv = attributeName.trim().split("/");

        if (attributeNameCnv.length == 1) {
            dirName = "/";
        } else {
            String sep = "/";
            for (int i = 0; i < (attributeNameCnv.length - 1); i++) {
                if (attributeNameCnv[i].trim().equals("")) continue;
                dirName =  dirName + sep + attributeNameCnv[i];
            }
            if (attributeName.indexOf("/") == 0) {
                if (dirName.trim().equals("")) dirName = "/";
            }
        }
        if (dirMap.containsKey(dirName) == false) return meta;
        Map dirDt = dirMap.getMap(dirName);
        meta.put("attribute", dirDt.get(attributeName));

        return meta;
    }

}

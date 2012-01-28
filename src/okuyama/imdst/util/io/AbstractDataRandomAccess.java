package okuyama.imdst.util.io;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;

import okuyama.imdst.util.*;


/**
 * okuyamaが利用するデータファイルをOSのページキャッシュにのせるために定期的にデータファイルの先頭<br>
 * 規定バイト分読み込み強制的にページキャッシュにのせるようにする.<br>
 *
 * @author T.Okuyama
 * @license GPL(Lv3)
 */
abstract public class AbstractDataRandomAccess extends RandomAccessFile {

    protected File dataFile = null;

    protected DataFilePageCacheMapper dataCacheMapper = null;

    protected ValueCacheMap highReferenceFrequencyMap = null;
    protected ConcurrentHashMap highReferencedMapCopy = null;

    protected boolean execMappingFlg = ImdstDefine.pageCacheMappendFlg;

    protected static int pageCacheMappendSize = ImdstDefine.pageCacheMappendSize;



    public AbstractDataRandomAccess(File target, String type) throws FileNotFoundException {
        super(target, type);
        this.dataFile = target;
        try {
            if(execMappingFlg) {
                this.highReferencedMapCopy = new ConcurrentHashMap(pageCacheMappendSize);
                this.highReferenceFrequencyMap = new ValueCacheMap(pageCacheMappendSize, this.highReferencedMapCopy);
                this.dataCacheMapper = new DataFilePageCacheMapper(target);
                this.dataCacheMapper.start();
            }
        } catch (Exception e) {}
    }

    abstract public void setDataPointMap(Map dataPointMap) ;


    public void close() throws IOException {
        try {
            if (execMappingFlg) {
                this.dataCacheMapper.close();
                this.dataCacheMapper.join(3000);
            }
            super.close();
        } catch (Exception e) {}        
    }

    public void putHighReferenceData(long seekPoint) {
        if (execMappingFlg) {
            highReferenceFrequencyMap.put(new Long(seekPoint), null);
        }
    }


   class DataFilePageCacheMapper extends Thread {

        private boolean runFlg = false;
        private File dataFile = null;

        public DataFilePageCacheMapper(File dataFile) {
            this.dataFile = dataFile;
            this.runFlg = true;
        }

        public void run() {
            while(runFlg) {
                try {
                    Thread.sleep(2500);
                    long start = System.nanoTime();
                    RandomAccessFile raf = new RandomAccessFile(this.dataFile, "r");
                    byte[] data = new byte[ImdstDefine.dataFileWriteMaxSize];  
                    
                    Set entrySet = highReferencedMapCopy.entrySet();
                    Iterator entryIte = entrySet.iterator(); 
                    int count = 0;
                    while(entryIte.hasNext()) {
            
                        Map.Entry obj = (Map.Entry)entryIte.next();
                        if (obj == null) continue;
                        try {
                            Long seekPoint = (Long)obj.getKey();
                            raf.seek(seekPoint.longValue());
                            raf.read(data, 0, ImdstDefine.dataFileWriteMaxSize);
                            if ((count % 10) == 0) {
                                if (runFlg == false) break;
                                Thread.sleep(30);
                            }
                        } catch (IOException e) {
                            highReferenceFrequencyMap.remove(obj.getKey());
                            highReferencedMapCopy.remove(obj.getKey());
                        }
                        count++;
                    }

                    data = null;
                    raf.close();
                    raf = null; 
                    long end = System.nanoTime();
                    //System.out.println((end - start) / 1000 / 1000);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        public void close() {
            this.runFlg = false;
        }
   }
}

package org.okuyama.hadoop.format;

import java.util.*;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

import org.okuyama.hadoop.config.*;

public class OkuyamaInputFormat extends InputFormat {

    public RecordReader<String, Text> createRecordReader(InputSplit split, TaskAttemptContext context) {

        return new OkuyamaRecordReader(split, context);
    }

    public List<InputSplit> getSplits(JobContext context) {

        List list = new ArrayList(16);
        String[] masterNodeInfos = OkuyamaHadoopFormatConfigure.getMasterNodeInfoList();
        String[] tags = OkuyamaHadoopFormatConfigure.getTagList();

        for (int idx = 0; idx < tags.length; idx++) {
            list.add(new OkuyamaInputSplit(tags[idx], masterNodeInfos));
        }

        return list;
    }
}


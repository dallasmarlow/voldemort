package co.vine.voldemort;

import java.util.Arrays;
import java.util.Collections;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import voldemort.store.readonly.mr.AbstractHadoopStoreBuilderMapper;

// `String2Strings` mapper expects each line to contain a tab seperated key and value,
// where the value is series of strings delimited by a space character.
public class String2Strings extends AbstractHadoopStoreBuilderMapper<LongWritable, Text> {
    @Override
    public Object makeKey(LongWritable key, Text value) {
        String val = value.toString();

        if (val.length() > 0) {
            return val.split("\t")[0];
        } else {
        	return "";
        }
    }

    @Override
    public Object makeValue(LongWritable key, Text value) {
    	String[] entries = value.toString().split("\t");

        if (entries.length > 1) {
        	return Arrays.asList(entries[1].split(" "));
        } else {
        	return Collections.<String>emptyList();
        }
    }
}

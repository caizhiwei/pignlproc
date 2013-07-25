package pignlproc.format;

import edu.umass.cs.iesl.wikilink.expanded.data.Mention;
import edu.umass.cs.iesl.wikilink.expanded.data.WikiLinkItem;
import edu.umass.cs.iesl.wikilink.expanded.data.WikiLinkItem$;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import scala.collection.Seq;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.net.URL;
import java.util.zip.GZIPInputStream;

/**
 * @author Zhiwei
 */
public class WikiLinkInputFormat extends FileInputFormat<Integer, Seq<Mention>> {

    public static class WikiLinkRecordReader extends RecordReader<Integer,Seq<Mention> > {
        TBinaryProtocol fsin;
        Integer key;
        Seq<Mention> value;
        String html;
        public WikiLinkRecordReader(FileSplit split, TaskAttemptContext context)
                throws IOException {
            Path file = split.getPath();
            FileSystem fs = file.getFileSystem(context.getConfiguration());
            FSDataInputStream fsDataInputStream= fs.open(file);
            BufferedInputStream stream = new BufferedInputStream(new GZIPInputStream(fsDataInputStream), 2048);
            fsin = new TBinaryProtocol(new TIOStreamTransport(stream));
        }

        // to be used for testing
        public WikiLinkRecordReader(URL fileURL)
                throws IOException {
            Path path = new Path("file://", fileURL.getPath());
            FSDataInputStream fsDataInputStream= FileSystem.getLocal(new Configuration()).open(path);
            BufferedInputStream stream = new BufferedInputStream(new GZIPInputStream(fsDataInputStream), 2048);
            fsin = new TBinaryProtocol(new TIOStreamTransport(stream));
        }
        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            Path file = ((FileSplit)split).getPath();
            FileSystem fs = file.getFileSystem(context.getConfiguration());
            BufferedInputStream stream = new BufferedInputStream(new GZIPInputStream(fs.open(file)), 2048);
            fsin = new TBinaryProtocol(new TIOStreamTransport(stream));
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            try{
                WikiLinkItem.Immutable item = WikiLinkItem$.MODULE$.decode(fsin);//Get a webpage
                key = item.docId();
                value = item.mentions();
                html = item.content().dom().toString();
                return true;
            }catch (Exception e){
                return false;
            }
        }

        @Override
        public Integer getCurrentKey() throws IOException, InterruptedException {
            return key;
        }

        @Override
        public Seq<Mention> getCurrentValue() throws IOException, InterruptedException {
            return value;
        }

        public String getCurrentHTML() throws IOException, InterruptedException {
            return html;
        }
        @Override
        public float getProgress() throws IOException, InterruptedException {
            return 0;
        }

        @Override
        public void close() throws IOException {
        }
    }
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    @Override
    public RecordReader<Integer, Seq<Mention>> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new WikiLinkRecordReader((FileSplit)inputSplit,taskAttemptContext);
    }
}

package pignlproc.storage;

import de.l3s.boilerpipe.BoilerpipeProcessingException;
import de.l3s.boilerpipe.extractors.DefaultExtractor;
import edu.umass.cs.iesl.wikilink.expanded.data.Context;
import edu.umass.cs.iesl.wikilink.expanded.data.Mention;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.pig.*;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.PigSplit;
import org.apache.pig.data.*;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import pignlproc.format.WikiLinkInputFormat;
import scala.collection.Iterator;
import scala.collection.Seq;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Zhiwei
 */
public class WikiLinkLoader extends LoadFunc implements LoadMetadata {

    @Override
    public ResourceSchema getSchema(String location, Job job) throws IOException {
        Schema schema = new Schema();
        schema.add(new Schema.FieldSchema("id", DataType.INTEGER));

        Schema mentionSchema = new Schema();
        mentionSchema.add(new Schema.FieldSchema("anchorText", DataType.CHARARRAY));
        mentionSchema.add(new Schema.FieldSchema("wikiUrl", DataType.CHARARRAY));
        mentionSchema.add(new Schema.FieldSchema("context", DataType.CHARARRAY));
        mentionSchema.add(new Schema.FieldSchema("begin", DataType.INTEGER));
        mentionSchema.add(new Schema.FieldSchema("end", DataType.INTEGER));
        Schema mentionWrapper = new Schema(new Schema.FieldSchema("t", mentionSchema));
        mentionWrapper.setTwoLevelAccessRequired(true);
        schema.add(new Schema.FieldSchema("mentions", mentionWrapper, DataType.BAG));

        schema.add(new Schema.FieldSchema("articleText", DataType.CHARARRAY));

        return new ResourceSchema(schema);
    }

    @Override
    public ResourceStatistics getStatistics(String location, Job job) throws IOException {
        return null;
    }

    @Override
    public String[] getPartitionKeys(String location, Job job) throws IOException {
        return null;
    }

    @Override
    public void setPartitionFilter(Expression partitionFilter) throws IOException {
    }




    @Override
    public void setLocation(String location, Job job) throws IOException {
        FileInputFormat.setInputPaths(job, location);
    }

    @Override
    public InputFormat getInputFormat() throws IOException {
        return new WikiLinkInputFormat();
    }

    protected WikiLinkInputFormat.WikiLinkRecordReader reader;

    protected TupleFactory tupleFactory;

    protected BagFactory bagFactory;

    @Override
    public void prepareToRead(RecordReader reader, PigSplit split) throws IOException {
        this.reader = (WikiLinkInputFormat.WikiLinkRecordReader) reader;
        tupleFactory = TupleFactory.getInstance();
        bagFactory = BagFactory.getInstance();
    }

    @Override
    public Tuple getNext() throws IOException {
        try {
            boolean next = reader.nextKeyValue();
            if (!next) {
                return null;
            }

            Integer docId = reader.getCurrentKey();
            Seq<Mention> mentions = reader.getCurrentValue();
            DataBag mentionBag = bagFactory.newDefaultBag();
            Iterator<Mention> iter = mentions.iterator();
            while(iter.hasNext()) {
                Mention mention = iter.next();
                if(!mention.context().isEmpty()){
                    Context context = mention.context().get();
                    int beg = context.left().length();
                    int end = beg+context.middle().length();
                    mentionBag.add(tupleFactory.newTupleNoCopy(Arrays.asList(mention.anchorText(),
                            mention.wikiUrl(),context.left()+context.middle()+context.right(),beg,end)));
                }
            }
            String raw = reader.getCurrentHTML();
            String article = null;
            try {
                article = DefaultExtractor.INSTANCE.getText(raw);
            } catch (Exception e) {
            }
            return tupleFactory.newTupleNoCopy(Arrays.asList(docId,mentionBag,article));
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}

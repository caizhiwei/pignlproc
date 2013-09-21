package pignlproc.storage;

import de.l3s.boilerpipe.extractors.DefaultExtractor;
import org.apache.hadoop.mapreduce.Job;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author Zhiwei
 * Load wikilink data set with full html pages
 */
public class WikiLinkFullLoader extends WikiLinkLoader{
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
    public Tuple getNext() throws IOException {
        try {
            Tuple tuple = super.getNext();
            if(tuple == null)
                return null;
            String raw = reader.getCurrentHTML();
            String article = null;
            try {
                article = DefaultExtractor.INSTANCE.getText(raw);
            } catch (Exception e) {
            }
            List list=new LinkedList();
            for(Object o:tuple.getAll()){
                list.add(o);
            }
            list.add(article);
            return tupleFactory.newTupleNoCopy(list);
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }
}

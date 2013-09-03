package pignlproc.helpers;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.schema.Schema.FieldSchema;

import java.io.IOException;

/**
 /**
 * @author Zhiwei
 * Take a bag of paragraphs and emit a single tuple with a single field
 *
 */

public class ExtractLinkSF extends EvalFunc<String> {
    @Override
    public String exec (Tuple input) throws IOException {

        Object input1 = input.get(0);
        if (input1 == null) {
            return null;
        }
        String in = input1.toString();

        int index = in.lastIndexOf('/');
        if(index!=-1)
            return in.substring(index+1);
        else
            return in;
    }

    @Override
    public Schema outputSchema(Schema input) {
        return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
    }
}


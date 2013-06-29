package pignlproc.storage;

import org.apache.pig.PigServer;
import org.apache.pig.data.Tuple;
import org.junit.Test;

import java.net.URL;
import java.util.Iterator;

import static org.apache.pig.ExecType.LOCAL;
import static org.junit.Assert.assertEquals;

/**
 * @author Zhiwei
 */
public class TestWikiLinkLoader {
    @Test
    public void testWikiLinkLoader() throws Exception,
            InterruptedException {
        URL wikiLinkData = Thread.currentThread().getContextClassLoader().getResource("wikilink-small");
        String filename = wikiLinkData.getPath();
        PigServer pig = new PigServer(LOCAL);
        filename = filename.replace("\\", "\\\\");
        String query = "A = LOAD 'file://" + filename
                + "' USING pignlproc.storage.WikiLinkLoader() as (id, mentions);";
        System.out.println(query);
        pig.registerQuery(query);
        Iterator<Tuple> it = pig.openIterator("A");
        int tupleCount = 0;
        while (it.hasNext()) {
            Tuple tuple = it.next();
            if (tuple == null) {
                throw new Exception("got unexpected null tuple");
            } else {
                if (tuple.size() > 0) {
                    tupleCount++;
                }
            }
        }
        assertEquals(25, tupleCount);
    }
}

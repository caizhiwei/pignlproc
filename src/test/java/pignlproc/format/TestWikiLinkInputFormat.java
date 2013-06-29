package pignlproc.format;

import org.junit.Test;

import java.io.IOException;
import java.net.URL;

import static org.junit.Assert.assertEquals;

/**
 * @author Zhiwei
 */
public class TestWikiLinkInputFormat {
    @Test
    public void testRecordReader() throws IOException,
            InterruptedException {
        URL wikiLinkData = Thread.currentThread().getContextClassLoader().getResource("wikilink-small/wikilink-small1.thrift.gz");
        WikiLinkInputFormat.WikiLinkRecordReader reader = new WikiLinkInputFormat.WikiLinkRecordReader(wikiLinkData);
        for(int i=0;i<5;i++){
            reader.nextKeyValue();
            assertEquals(i + 100000, reader.getCurrentKey());
        }
        assertEquals(false, reader.nextKeyValue());
    }

}

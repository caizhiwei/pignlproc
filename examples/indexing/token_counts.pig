/*
 * Token counts associated with Wikipedia concepts.
 *
 * @params $OUTPUT_DIR - the directory where the files should be stored
 *         $STOPLIST_PATH - the location of the stoplist in HDFS		
 *         $STOPLIST_NAME - the filename of the stoplist
 *         $INPUT - the wikipedia XML dump
 *         $MIN_COUNT - the minumum count for a token to be included in the index
 *         $PIGNLPROC_JAR - the location of the pignlproc jar
 *         $LANG - the language of the Wikidump 
 *         $MAX_SPAN_LENGTH - the maximum length (in chars) for a paragraph span
 *         $ANALYZER_NAME - the name of the language specific Lucene Analyzer - i.e. "EnglishAnalyzer"
 */


SET job.name 'DBpedia Spotlight: Token counts per URI for $LANG'

%default DEFAULT_PARALLEL 20
SET default_parallel $DEFAULT_PARALLEL

SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec gz

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR;

-- Define alias for tokenizer function
DEFINE tokens pignlproc.index.GetCountsLucene('$STOPLIST_PATH','$STOPLIST_NAME','$LANG','$ANALYZER_NAME');

DEFINE textWithLink pignlproc.evaluation.ParagraphsWithLink('$MAX_SPAN_LENGTH');
DEFINE JsonCompressedStorage pignlproc.storage.JsonCompressedStorage();


IMPORT '$MACROS_DIR/nerd_commons.pig';

-- Get articles (IDs and pairs are not used (and not produced))
ids, articles, pairs = readWikipedia('$INPUT', '$LANG', $MIN_SURFACE_FORM_LENGTH);

-- Extract paragraph contexts of the links
paragraphs = FOREACH articles GENERATE
  pageUrl,
  FLATTEN(textWithLink(text, links, paragraphs))
  AS (paragraphIdx, paragraph, targetUri, startPos, endPos);

--Changes for indexing on small cluster
contexts = FOREACH paragraphs GENERATE
  targetUri AS uri,
  paragraph AS paragraph;

freq_sorted = token_count(contexts, $MIN_CONTEXTS, $MIN_COUNT);

STORE freq_sorted INTO '$OUTPUT_DIR' USING PigStorage('\t');


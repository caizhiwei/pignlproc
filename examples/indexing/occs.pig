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

%default minCount 3

-- Define alias for tokenizer function
DEFINE tokens pignlproc.index.GetCountsLucene('$STOPLIST_PATH','$STOPLIST_NAME','$LANG','$ANALYZER_NAME');

DEFINE ExtractLinkSF pignlproc.helpers.ExtractLinkSF();

DEFINE JsonCompressedStorage pignlproc.storage.JsonCompressedStorage();



IMPORT '$MACROS_DIR/nerd_commons.pig';

-- Get articles (IDs and pairs are not used (and not produced))
ids, articles, pairs = readWikipedia('$INPUT', '$LANG', $MIN_SURFACE_FORM_LENGTH);

-- Extract paragraph contexts of the links
occs = FOREACH articles GENERATE
  pageUrl,
  FLATTEN(links);
occs = FOREACH occs GENERATE
  ExtractLinkSF(pageUrl) AS src,
  ExtractLinkSF(links::target) AS tar;

-- Store occurrence pairs
STORE occs INTO '$OUTPUT_DIR/occs' USING PigStorage('\t');

-- GET occurrence counts
groupedOccs = group occs BY (src,tar);
cnt = Foreach groupedOccs GENERATE group.src AS src,group.tar AS tar,COUNT(occs) AS count;
reducedCnt = FILTER cnt BY count>=$minCount;

-- Group into a ajancency list
adjLists = GROUP reducedCnt BY src;

-- Format to a JSON like format
JSONAdjLists = FOREACH adjLists GENERATE group AS src,reducedCnt.(tar,count) AS counts;

describe JSONAdjLists;

-- Write out
-- Consider use JSONStorage
STORE JSONAdjLists INTO '$OUTPUT_DIR/occs-count' USING PigStorage();

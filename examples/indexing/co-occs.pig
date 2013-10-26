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
%default inFile $OUTPUT_DIR/occs
%default useDocLevel false
%default minCount 3

SET default_parallel $DEFAULT_PARALLEL

SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec gz

-- Register the project jar to use the custom loaders and UDFs
REGISTER $PIGNLPROC_JAR;

-- Define alias for tokenizer function
DEFINE tokens pignlproc.index.GetCountsLucene('$STOPLIST_PATH','$STOPLIST_NAME','$LANG','$ANALYZER_NAME');

DEFINE ExtractLinkSF pignlproc.helpers.ExtractLinkSF();

DEFINE JsonCompressedStorage pignlproc.storage.JsonCompressedStorage();


IMPORT '$MACROS_DIR/nerd_commons.pig';


-- Go through all the occurrences in the TSV file
occs = LOAD '$inFile' USING PigStorage('\t') AS (src:chararray,tar:chararray);

coOccsById = GROUP occs BY src;

-- Flatten by itself to produce cross product for the list of uri
entityPairs = FOREACH coOccsById generate
   flatten(occs.tar) AS (src) ,
   flatten(occs.tar) AS (tar) ;

-- Self co-occurrences are not desired
cleanedPairs = FILTER entityPairs BY src!= tar;

--Group by entity co-occured pair
groupedPairs = GROUP cleanedPairs BY (src,tar);

-- Count the entity pairs
cnt = FOREACH groupedPairs GENERATE group.src,group.tar, COUNT(cleanedPairs) AS count;

-- Cooccurrences less than $minCount will be removed
reducedCnt = FILTER cnt BY count>=$minCount;

-- Group into a ajancency list
adjLists = GROUP reducedCnt BY src;

-- Format to a JSON like format
JSONAdjLists = FOREACH adjLists GENERATE group AS src,reducedCnt.(tar,count) AS counts;

describe JSONAdjLists;

-- Write out
-- Consider use JSONStorage
STORE JSONAdjLists INTO '$OUTPUT_DIR/co-occs-count' USING PigStorage();

SET job.name 'DBpedia Spotlight: Test wikilink dataset';

SET default_parallel $DEFAULT_PARALLEL;

REGISTER $PIGNLPROC_JAR;

DEFINE dbpediaEncode pignlproc.evaluation.DBpediaUriEncode('en');
DEFINE resolve pignlproc.helpers.SecondIfNotNullElseFirst();
DEFINE tokens pignlproc.index.GetCountsLucene('$STOPLIST_PATH','$STOPLIST_NAME','en','EnglishAnalyzer');
DEFINE ngramGenerator pignlproc.helpers.RestrictedNGramGenerator('$MAX_NGRAM_LENGTH', '', 'en_US'); -- do not restrict: ''

IMPORT '$MACROS_DIR/nerd_commons.pig';


origin = LOAD '$INPUT'
  USING pignlproc.storage.WikiLinkLoader()
  AS (docId, mention);
mentions = FOREACH origin GENERATE
  docId, FLATTEN(mention);
pairs = FOREACH mentions GENERATE
  anchorText AS surfaceForm, docId AS pageUrl, wikiUrl AS uri, context;
storeSurfaceForm(pairs,'$TEMPORARY_SF_LOCATION');
EXEC;
DEFINE ngramGenerator pignlproc.helpers.RestrictedNGramGenerator('$MAX_NGRAM_LENGTH', '$TEMPORARY_SF_LOCATION/surfaceForms', '$LOCALE');
pageNgrams = FOREACH pairs GENERATE
  FLATTEN(ngramGenerator(context)) AS ngram,
  pageUrl PARALLEL 40;
pageNgrams = DISTINCT pageNgrams;

-- Count
uriCounts, sfCounts, pairCounts, ngramCounts = count(pairs, pageNgrams);


--------------------
-- join some results
--------------------

-- Join annotated and unannotated SF counts:
sfAndTotalCounts = FOREACH (JOIN
  sfCounts    BY surfaceForm LEFT OUTER,
  ngramCounts BY ngram) GENERATE surfaceForm, sfCount, ngramCount;


--------------------
-- Output
--------------------

STORE pairCounts INTO '$OUTPUT/pairCounts';
STORE uriCounts INTO '$OUTPUT/uriCounts';
STORE sfAndTotalCounts INTO '$OUTPUT/sfAndTotalCounts';
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
  FLATTEN(mention);
--Changes for indexing on small cluster

contexts = FOREACH mentions GENERATE
  wikiUrl AS uri,
  context AS paragraph;

freq_sorted = token_count(contexts, $MIN_CONTEXTS, $MIN_COUNT);

STORE freq_sorted INTO '$OUTPUT' USING PigStorage('\t');


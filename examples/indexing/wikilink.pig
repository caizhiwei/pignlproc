SET job.name 'DBpedia Spotlight: Test wikilink dataset';

SET default_parallel $DEFAULT_PARALLEL;

REGISTER $PIGNLPROC_JAR;

DEFINE dbpediaEncode pignlproc.evaluation.DBpediaUriEncode('en');
DEFINE resolve pignlproc.helpers.SecondIfNotNullElseFirst();
DEFINE tokens pignlproc.index.GetCountsLucene('$STOPLIST_PATH','$STOPLIST_NAME','en','EnglishAnalyzer');
DEFINE ngramGenerator pignlproc.helpers.RestrictedNGramGenerator('$MAX_NGRAM_LENGTH', '', 'en_US'); -- do not restrict: ''

IMPORT '$MACROS_DIR/nerd_commons.pig';

-- Load Mentions from wiki-link dataset
articles, pairs = readWikilink('$INPUT');

--Changes for indexing on small cluster
contexts = FOREACH pairs GENERATE
  uri,
  context AS paragraph;

freq_sorted = token_count(contexts, $MIN_CONTEXTS, $MIN_COUNT);

STORE freq_sorted INTO '$OUTPUT' USING PigStorage('\t');

EXEC;

storeSurfaceForm(pairs,'$TEMPORARY_SF_LOCATION');
EXEC;
pageNgrams = memoryIntensiveNgrams(articles, $MAX_NGRAM_LENGTH, '$TEMPORARY_SF_LOCATION', $LOCALE);


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

SET job.name 'DBpedia Spotlight: Test wikilink dataset';

SET default_parallel $DEFAULT_PARALLEL;

REGISTER $PIGNLPROC_JAR;

DEFINE dbpediaEncode pignlproc.evaluation.DBpediaUriEncode('en');
DEFINE resolve pignlproc.helpers.SecondIfNotNullElseFirst();
DEFINE tokens pignlproc.index.GetCountsLucene('$STOPLIST_PATH','$STOPLIST_NAME','en','EnglishAnalyzer');
DEFINE ngramGenerator pignlproc.helpers.RestrictedNGramGenerator('$MAX_NGRAM_LENGTH', '', 'en_US'); -- do not restrict: ''

-- Load Mentions from wiki-link dataset
idAndMentions = LOAD '$INPUT'
   USING pignlproc.storage.WikiLinkLoader()
   AS (id, mentions);
allMentions = FOREACH idAndMentions 
   GENERATE FLATTEN(mentions);
uriContext = FOREACH allMentions
   GENERATE anchorText as surfaceForm, wikiUrl as uri,context,begin,end;


--Changes for indexing on small cluster
contexts = FOREACH uriContext GENERATE
  uri,
  context AS paragraph;

-- this is reduce #1
by_uri = GROUP contexts by uri;

min_contexts = FILTER by_uri BY (COUNT(contexts) >=$MIN_CONTEXTS);

paragraph_bag = FOREACH min_contexts GENERATE
	group AS uri,
	contexts.paragraph AS paragraphs;

--TOKENIZE, REMOVE STOPWORDS AND COUNT HERE
contexts = FOREACH paragraph_bag GENERATE
	uri, tokens(paragraphs) AS tokens;

freq_sorted = FOREACH contexts {
	unsorted = tokens.(token, count);
    filtered = FILTER unsorted BY (count >= $MIN_COUNT);
	-- sort descending
	sorted = ORDER filtered BY count desc;
	GENERATE
	  uri, sorted;
}

STORE freq_sorted INTO '$OUTPUT_TOKEN' USING PigStorage('\t');


pageNgrams = FOREACH uriContext GENERATE
    FLATTEN(ngramGenerator(context)) AS ngram,
    uri as pageUrl;
doubledLinks = FOREACH uriContext GENERATE
    surfaceForm,uri;
-- Count
    -- Count pairs
    pairGrp = GROUP doubledLinks BY (surfaceForm, uri);
    pairCounts = FOREACH pairGrp GENERATE
      FLATTEN($0) AS (pairSf, pairUri),
      COUNT($1) AS pairCount;

    -- Count surface forms
    sfGrp = GROUP doubledLinks BY surfaceForm;
    sfCounts = FOREACH sfGrp GENERATE
      $0 AS surfaceForm,
      COUNT($1) AS sfCount;

    -- Count URIs
    uriGrp = GROUP doubledLinks BY uri;
    uriCounts = FOREACH uriGrp GENERATE
      $0 AS uri,
      COUNT($1) AS uriCount;

    -- Count Ngrams
    ngrams = FOREACH pageNgrams GENERATE
      ngram;
    ngramGrp = GROUP ngrams BY ngram;
    ngramCounts = FOREACH ngramGrp GENERATE
      $0 as ngram,
      COUNT($1) AS ngramCount;

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

STORE pairCounts INTO '$OUTPUT_NE/pairCounts';
STORE uriCounts INTO '$OUTPUT_NE/uriCounts';
STORE sfAndTotalCounts INTO '$OUTPUT_NE/sfAndTotalCounts';

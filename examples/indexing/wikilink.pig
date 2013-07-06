SET job.name 'DBpedia Spotlight: Test wikilink dataset';

SET default_parallel $DEFAULT_PARALLEL;

REGISTER $PIGNLPROC_JAR;

DEFINE dbpediaEncode pignlproc.evaluation.DBpediaUriEncode('en');
DEFINE resolve pignlproc.helpers.SecondIfNotNullElseFirst();

-- Load Mentions from wiki-link dataset
idAndMentions = LOAD '$INPUT'
   USING pignlproc.storage.WikiLinkLoader()
   AS (id, mentions);
allMentions = FOREACH idAndMentions 
   GENERATE FLATTEN(mentions);
uriContext = FOREACH allMentions
   GENERATE anchorText as surfaceForm, dbpediaEncode(wikiUrl) as uri,context,begin,end;
rawRedirects = LOAD '$REDIRECT'
   USING PigStorage(' ')
   AS (source,relation,target);
redirects = FOREACH rawRedirects
   GENERATE REPLACE(source,'<|>','') as source,REPLACE(target,'<|>','') as target;
pageLinksRedirectsJoin = JOIN
   redirects BY source RIGHT,
   uriContext BY uri;
resolvedLinks = FOREACH pageLinksRedirectsJoin GENERATE
      surfaceForm,
      FLATTEN(resolve(uri, target)) AS uri,
      context,begin,end;
STORE resolvedLinks INTO '$OUTPUT_DIR' USING PigStorage('\t');

SET job.name 'DBpedia Spotlight: Test wikilink dataset';

SET default_parallel $DEFAULT_PARALLEL;

REGISTER $PIGNLPROC_JAR;

-- Load Mentions from wiki-link dataset
idAndMentions = LOAD '$INPUT'
   USING pignlproc.storage.WikiLinkLoader()
   AS (id, mentions);
allMentions = FOREACH idAndMentions 
   GENERATE FLATTEN(mentions);
STORE allMentions INTO '$OUTPUT_DIR' USING PigStorage('\t');

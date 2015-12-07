CREATE EXTENSION pgduma;

CREATE TABLE lawsearch_test (i integer, t tsvector[]);
\copy lawsearch_test from 'data/pgduma.data'

SELECT i FROM lawsearch_test WHERE t @@ 'wr|qh' ORDER BY t >< 'wr|qh' LIMIT 10;
SELECT i FROM lawsearch_test WHERE t @@ 'eq&yt' ORDER BY t >< 'eq&yt' LIMIT 10;
SELECT i FROM lawsearch_test WHERE t @@ '(eq&yt)|(wr&qh)' ORDER BY t >< '(eq&yt)|(wr&qh)' LIMIT 10;

CREATE INDEX lawsearch_test_idx ON lawsearch_test USING gin (t);

SET enable_seqscan TO off;

SELECT i FROM lawsearch_test WHERE t @@ 'wr|qh' ORDER BY t >< 'wr|qh' LIMIT 10;
SELECT i FROM lawsearch_test WHERE t @@ 'eq&yt' ORDER BY t >< 'eq&yt' LIMIT 10;
SELECT i FROM lawsearch_test WHERE t @@ '(eq&yt)|(wr&qh)' ORDER BY t >< '(eq&yt)|(wr&qh)' LIMIT 10;

VACUUM lawsearch_test;
DELETE FROM lawsearch_test;
\copy lawsearch_test from 'data/pgduma.data'
VACUUM lawsearch_test;

SELECT i FROM lawsearch_test WHERE t @@ 'wr|qh' ORDER BY t >< 'wr|qh' LIMIT 10;
SELECT i FROM lawsearch_test WHERE t @@ 'eq&yt' ORDER BY t >< 'eq&yt' LIMIT 10;
SELECT i FROM lawsearch_test WHERE t @@ '(eq&yt)|(wr&qh)' ORDER BY t >< '(eq&yt)|(wr&qh)' LIMIT 10;

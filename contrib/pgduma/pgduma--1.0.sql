/* pgduma/pgduma--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pgduma" to load this file. \quit

CREATE FUNCTION tsvector_array_match_tsquery(tsvector[], tsquery)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tsvector_array_tsquery_distance(tsvector[], tsquery)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR @@ (
    PROCEDURE = tsvector_array_match_tsquery,
    LEFTARG = tsvector[], 
    RIGHTARG = tsquery,
    RESTRICT = tsmatchsel,
    JOIN = tsmatchjoinsel);

CREATE OPERATOR >< (
    PROCEDURE = tsvector_array_tsquery_distance,
    LEFTARG = tsvector[], 
    RIGHTARG = tsquery);

CREATE FUNCTION gin_extract_tsvector_array(tsvector[], internal, internal)
RETURNS internal
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gin_tsvector_array_consistent(internal, smallint, tsquery, integer, internal, internal, internal, internal)
RETURNS boolean
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION gin_tsvector_array_distance(internal, smallint, tsquery, integer, internal, internal, internal, internal, internal)
RETURNS float8
AS 'MODULE_PATHNAME'
LANGUAGE C IMMUTABLE STRICT;

CREATE OPERATOR CLASS tsvector_array_ops DEFAULT
	FOR TYPE tsvector[] USING gin AS
	OPERATOR 1  @@(tsvector[], tsquery),
	OPERATOR 3  ><(tsvector[], tsquery) FOR ORDER BY float_ops,
	FUNCTION 1  gin_cmp_tslexeme(text, text),
	FUNCTION 2  gin_extract_tsvector_array(tsvector[], internal, internal),
	FUNCTION 3  gin_extract_tsquery(tsquery, internal, smallint, internal, internal, internal, internal),
	FUNCTION 4  gin_tsvector_array_consistent(internal, smallint, tsquery, integer, internal, internal, internal, internal),
	FUNCTION 5  gin_cmp_prefix(text, text, smallint, internal),
	FUNCTION 6  gin_tsvector_config(internal),
	FUNCTION 7  gin_tsquery_pre_consistent(internal, smallint, tsquery, integer, internal, internal, internal, internal),
	FUNCTION 8  gin_tsvector_array_distance(internal, smallint, tsquery, integer, internal, internal, internal, internal, internal),
	STORAGE text;

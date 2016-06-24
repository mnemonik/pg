/* contrib/pg_part/pg_part--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_part" to load this file. \quit

--
-- PostgreSQL partition extension
--

CREATE SCHEMA pg_part;

--
-- pg_part._get_attname_by_attnum()
--
-- Get an attribute name by nspname, relname and attribute number
--
CREATE OR REPLACE FUNCTION pg_part._get_attname_by_attnum (
  NAME,
  NAME,
  SMALLINT
) RETURNS NAME
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _attnum ALIAS  FOR $3;
  _attname NAME;
BEGIN
  SELECT a.attname INTO _attname
    FROM pg_namespace n, pg_class c, pg_attribute a
   WHERE n.nspname = _nspname
     AND c.relname = _relname
     AND n.oid = c.relnamespace
     AND c.oid = a.attrelid
     AND a.attnum = _attnum;

  RETURN _attname;
END;
' LANGUAGE 'plpgsql';

--
-- pg_part._get_primary_key_def()
--
-- Get a primary key definition string for new partition.
--
CREATE OR REPLACE FUNCTION pg_part._get_primary_key_def (
  NAME,
  NAME,
  NAME
) RETURNS TEXT
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _conname NAME;
  _conkey SMALLINT[];
  _size SMALLINT;
  _keyidx SMALLINT;
  _keyname NAME;
  _keys TEXT;
BEGIN
  SELECT a.conname, a.conkey, array_length(a.conkey, 1)
    INTO _conname, _conkey, _size
    FROM pg_namespace n, pg_class c, pg_constraint a
   WHERE n.nspname = _nspname
     AND c.relname = _relname
     AND n.oid = c.relnamespace
     AND c.oid = a.conrelid
     AND a.contype = ''p'';

  IF NOT FOUND THEN
    RETURN '''';
  END IF;

  _keys = '''';
  
  FOR _keyidx IN 1.._size LOOP
    SELECT pg_part._get_attname_by_attnum(_nspname::name, _relname::name, _conkey[_keyidx]::smallint)
      INTO _keyname;

    _keys = _keys || '','' || _keyname;
  END LOOP;

  RETURN ''ALTER TABLE '' || _nspname || ''.'' || _partname || '' ADD PRIMARY KEY ('' || substring(_keys,2) || '')'';
END;
' LANGUAGE 'plpgsql';

--
-- pg_part._get_index_def()
--
-- Get index definition string(s) for new partition, excepting primary key.
--
CREATE OR REPLACE FUNCTION pg_part._get_index_def (
  NAME,
  NAME,
  NAME
) RETURNS SETOF TEXT
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _r RECORD;
  _indexname NAME;
  _indexdef TEXT;
BEGIN
  FOR _r IN SELECT indexdef,
                   replace(regexp_replace(regexp_replace(indexdef, ''.*\('', ''''), ''\).*'', ''''), '', '', ''_'') AS colname
              FROM pg_indexes
             WHERE schemaname = _nspname
               AND tablename = _relname
               AND indexname IN (
                   SELECT c2.relname
                     FROM pg_namespace n, pg_class c, pg_index i, pg_class c2
                    WHERE n.nspname = _nspname
                      AND c.relname = _relname
                      AND n.oid = c.relnamespace
                      AND c.oid = i.indrelid
                      AND i.indisprimary <> true
                      AND i.indexrelid = c2.oid
                   ) LOOP

    _indexname = _partname || ''_'' || _r.colname || ''_idx'';

    _indexdef = _r.indexdef;
    _indexdef = regexp_replace(_indexdef, ''INDEX .* ON '', ''INDEX '' || _indexname || '' ON '');
    _indexdef = replace(_indexdef, '' ON '' || _relname, '' ON '' || _nspname || ''.'' || _partname);
    
    RETURN NEXT _indexdef;
  END LOOP;

  RETURN;
END
' LANGUAGE 'plpgsql';

--
-- pg_part._get_partition_def()
--
-- Get a partiton definition string for new partition
--
CREATE OR REPLACE FUNCTION pg_part._get_partition_def (
  NAME,
  NAME,
  NAME,
  TEXT
) RETURNS TEXT
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _cond ALIAS FOR $4;
  _partition_def TEXT;
  _const_def TEXT;
BEGIN
  _const_def = pg_part._get_constraint_def(_partname, _cond);

  _partition_def = ''CREATE TABLE '' || _nspname || ''.'' || _partname || ''( '';
  _partition_def = _partition_def || ''CONSTRAINT '' || _const_def;
  _partition_def = _partition_def || '') INHERITS ('' || _nspname || ''.'' || _relname || '')'';

  RETURN _partition_def;
END;
' LANGUAGE 'plpgsql';

--
-- pg_part._get_export_query()
--
-- Get a query to export records with specified condition from parent table.
--
CREATE OR REPLACE FUNCTION pg_part._get_export_query (
  NAME,
  NAME,
  TEXT,
  TEXT
) RETURNS TEXT
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _cond ALIAS FOR $3;
  _temp_file ALIAS FOR $4;
  _query TEXT;
BEGIN
  _query = ''COPY ( '' || ''SELECT * FROM '' || _nspname || ''.'' || _relname || '' WHERE '' || _cond || '' ) to '''''' || _temp_file || '''''''';

  RETURN _query;
END;
' LANGUAGE 'plpgsql';

--
-- pg_part._get_import_query()
--
-- Get a query to import records into specified partition
--
CREATE OR REPLACE FUNCTION pg_part._get_import_query (
  NAME,
  NAME,
  TEXT
) RETURNS TEXT
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _temp_file ALIAS FOR $3;
  _query TEXT;
BEGIN
  _query = ''COPY '' || _nspname || ''.'' || _relname || '' FROM '''''' || _temp_file || '''''''';

  RETURN _query;
END;
' LANGUAGE 'plpgsql';

--
-- pg_part.add_partition()
--
-- Add a new partition with a specified condition
--
CREATE OR REPLACE FUNCTION pg_part.add_partition (
  NAME,
  NAME,
  NAME,
  TEXT,
  TEXT
) RETURNS BOOLEAN
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _cond ALIAS FOR $4;
  _temp_file ALIAS FOR $5;
  _r RECORD;
  _def TEXT;
BEGIN
  FOR _r IN SELECT pg_part._get_partition_def(_nspname, _relname, _partname, _cond) LOOP
    _def = _r._get_partition_def || '';'';
    RAISE NOTICE ''add_partition: %'', _def;
    EXECUTE _def;
  END LOOP;
  
  SELECT pg_part._get_export_query(_nspname, _relname, _cond, _temp_file)
    INTO _def;
  _def = _def || '';'';
  RAISE NOTICE ''add_partition: %'', _def;
  EXECUTE _def;

  _def = ''DELETE FROM '' || _nspname || ''.'' || _relname || '' WHERE '' || _cond;
  _def = _def || '';'';
  RAISE NOTICE ''add_partition: %'', _def;
  EXECUTE _def;

  SELECT pg_part._get_import_query(_nspname, _partname, _temp_file)
    INTO _def;
  _def = _def || '';'';
  RAISE NOTICE ''add_partition: %'', _def;
  EXECUTE _def;

  SELECT pg_part._get_primary_key_def(_nspname, _relname, _partname)
    INTO _def;
  _def = _def || '';'';
  RAISE NOTICE ''add_partition: %'', _def;
  EXECUTE _def;

  FOR _r IN SELECT pg_part._get_index_def(_nspname, _relname, _partname) LOOP
    _def = _r._get_index_def || '';'';
    RAISE NOTICE ''add_partition: %'', _def;
    EXECUTE _def;
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- pg_part.merge_partition()
--
-- Merge a partition into the parent table.
--
CREATE OR REPLACE FUNCTION pg_part.merge_partition (
  NAME,
  NAME,
  NAME,
  TEXT,
  TEXT
) RETURNS BOOLEAN
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _cond ALIAS FOR $4;
  _temp_file ALIAS FOR $5;
  _r RECORD;
  _def TEXT;
BEGIN
  SELECT pg_part._get_export_query(_nspname, _partname, ''1 = 1'', _temp_file)
    INTO _def;
  _def = _def || '';'';
  RAISE NOTICE ''merge_partition: %'', _def;
  EXECUTE _def;

  SELECT pg_part._get_import_query(_nspname, _relname, _temp_file)
    INTO _def;
  _def = _def || '';'';
  RAISE NOTICE ''merge_partition: %'', _def;
  EXECUTE _def;

  SELECT pg_part._get_detach_partition_def(_nspname, _relname, _partname)
    INTO _def;
  _def = _def || '';'';
  RAISE NOTICE ''merge_partition: %'', _def;
  EXECUTE _def;

  _def = ''DROP TABLE '' || _nspname || ''.'' || _partname;
  _def = _def || '';'';
  RAISE NOTICE ''merge_partition: %'', _def;
  EXECUTE _def;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- pg_part._get_constraint_name()
--
CREATE OR REPLACE FUNCTION pg_part._get_constraint_name (
  NAME
) RETURNS TEXT
AS '
DECLARE
  _partname ALIAS FOR $1;
BEGIN
  RETURN ''__'' || _partname || ''_check'';
END;
' LANGUAGE 'plpgsql';

--
-- pg_part._get_constraint_def()
--
CREATE OR REPLACE FUNCTION pg_part._get_constraint_def (
  NAME,
  TEXT
) RETURNS TEXT
AS '
DECLARE
  _partname ALIAS FOR $1;
  _cond ALIAS FOR $2;
BEGIN
  RETURN pg_part._get_constraint_name(_partname) || '' CHECK('' || _cond || '')'';
END;
' LANGUAGE 'plpgsql';

--
-- pg_part._get_attach_partition_def()
--
-- Get a definition string for attaching a partition
--
CREATE OR REPLACE FUNCTION pg_part._get_attach_partition_def (
  NAME,
  NAME,
  NAME,
  TEXT
) RETURNS TEXT
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _cond ALIAS FOR $4;
  _partition_def TEXT;
  _const_def TEXT;
BEGIN
  _const_def = pg_part._get_constraint_def(_partname, _cond);

  _partition_def = ''ALTER TABLE '' || _nspname || ''.'' || _partname;
  _partition_def = _partition_def || '' INHERIT '' || _nspname || ''.'' || _relname || '','';
  _partition_def = _partition_def || '' ADD CONSTRAINT '' || _const_def;

  RETURN _partition_def;
END;
' LANGUAGE 'plpgsql';

--
-- pg_part.attach_partition()
--
-- Attach a new partition to the parent table with a specified condition
--
CREATE OR REPLACE FUNCTION pg_part.attach_partition (
  NAME,
  NAME,
  NAME,
  TEXT
) RETURNS BOOLEAN
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _cond ALIAS FOR $4;
  _r RECORD;
  _def TEXT;
BEGIN
  --
  -- Check whether constraint is valid for this partition.
  --
  _def = ''SELECT count(*) FROM '' || _nspname || ''.'' || _partname || '' WHERE NOT ('' || _cond || '')'';
  RAISE NOTICE ''attach_partition: %'', _def;
  FOR _r IN EXECUTE _def LOOP
    IF _r.count > 0 THEN
      RAISE EXCEPTION ''attach_partition: % record(s) in this partition does not satisfy specified constraint.'', _r.count;
    END IF;
  END LOOP;

  FOR _r IN SELECT pg_part._get_attach_partition_def(_nspname, _relname, _partname, _cond) LOOP
    _def = _r._get_attach_partition_def || '';'';
    RAISE NOTICE ''attach_partition: %'', _def;
    EXECUTE _def;
  END LOOP;

--  SELECT pg_part._get_primary_key_def(_nspname, _relname, _partname)
--    INTO _def;
--  _def = _def || '';'';
--  RAISE NOTICE ''attach_partition: %'', _def;
--  EXECUTE _def;
--
--  FOR _r IN SELECT pg_part._get_index_def(_nspname, _relname, _partname) LOOP
--    _def = _r._get_index_def || '';'';
--    RAISE NOTICE ''attach_partition: %'', _def;
--    EXECUTE _def;
--  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';

--
-- pg_part._get_detach_partition_def()
--
-- Get a definition string for detaching a partition
--
CREATE OR REPLACE FUNCTION pg_part._get_detach_partition_def (
  NAME,
  NAME,
  NAME
) RETURNS TEXT
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _partition_def TEXT;
  _const_name TEXT;
BEGIN
  _const_name = pg_part._get_constraint_name(_partname);

  _partition_def = ''ALTER TABLE '' || _nspname || ''.'' || _partname;
  _partition_def = _partition_def || '' NO INHERIT '' || _nspname || ''.'' || _relname || '','';
  _partition_def = _partition_def || '' DROP CONSTRAINT '' || _const_name;

  RETURN _partition_def;
END;
' LANGUAGE 'plpgsql';

--
-- pg_part.detach_partition()
--
-- Detach a partition from the parent table.
--
CREATE OR REPLACE FUNCTION pg_part.detach_partition (
  NAME,
  NAME,
  NAME
) RETURNS BOOLEAN
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname ALIAS FOR $3;
  _r RECORD;
  _def TEXT;
BEGIN
  FOR _r IN SELECT pg_part._get_detach_partition_def(_nspname, _relname, _partname) LOOP
    _def = _r._get_detach_partition_def || '';'';
    RAISE NOTICE ''detach_partition: %'', _def;
    EXECUTE _def;
  END LOOP;

  RETURN true;
END
' LANGUAGE 'plpgsql';


CREATE OR REPLACE FUNCTION pg_part.show_partition (
  NAME,
  NAME
) RETURNS SETOF NAME
AS '
DECLARE
  _nspname ALIAS FOR $1;
  _relname ALIAS FOR $2;
  _partname NAME;
BEGIN
  FOR _partname IN SELECT c.relname 
                     FROM pg_namespace n, pg_class p, pg_inherits i, pg_class c
                    WHERE n.nspname=_nspname
                      AND n.oid=p.relnamespace
                      AND p.relname=_relname
                      AND p.oid=i.inhparent
                      AND i.inhrelid=c.oid
                    ORDER BY c.relname LOOP
    RETURN NEXT _partname;
  END LOOP;

END
' LANGUAGE 'plpgsql';

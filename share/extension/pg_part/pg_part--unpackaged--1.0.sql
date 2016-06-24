/* contrib/pg_part/pg_part--unpackaged--1.0.sql */

-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_part" to load this file. \quit

ALTER EXTENSION pg_part ADD schema pg_part;

ALTER EXTENSION pg_part ADD function pg_part._get_attname_by_attnum(NAME,NAME,SMALLINT);
ALTER EXTENSION pg_part ADD function pg_part._get_primary_key_def(NAME,NAME,NAME);
ALTER EXTENSION pg_part ADD function pg_part._get_index_def(NAME,NAME,NAME);
ALTER EXTENSION pg_part ADD function pg_part._get_partition_def(NAME,NAME,NAME,TEXT);
ALTER EXTENSION pg_part ADD function pg_part._get_export_query(NAME,NAME,TEXT,TEXT);
ALTER EXTENSION pg_part ADD function pg_part._get_import_query(NAME,NAME,TEXT);
ALTER EXTENSION pg_part ADD function pg_part.add_partition(NAME,NAME,NAME,TEXT,TEXT);
ALTER EXTENSION pg_part ADD function pg_part.merge_partition(NAME,NAME,NAME,TEXT,TEXT);
ALTER EXTENSION pg_part ADD function pg_part._get_constraint_name(NAME);
ALTER EXTENSION pg_part ADD function pg_part._get_constraint_def(NAME,TEXT);
ALTER EXTENSION pg_part ADD function pg_part._get_attach_partition_def(NAME,NAME,NAME,TEXT);
ALTER EXTENSION pg_part ADD function pg_part.attach_partition(NAME,NAME,NAME,TEXT);
ALTER EXTENSION pg_part ADD function pg_part._get_detach_partition_def(NAME,NAME,NAME);
ALTER EXTENSION pg_part ADD function pg_part.detach_partition(NAME,NAME,NAME);
ALTER EXTENSION pg_part ADD function pg_part.show_partition(NAME,NAME);

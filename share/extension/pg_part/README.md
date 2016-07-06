# pg
PostreSQL extensions

## pg_part - partition extension for PostgreSQL

### About
=============
Хочу добиться этим расширением автоматического партицирования


partition.add
--------------------

partition.add() function creates new partition ...

    partition.add(schema, table, part, field, from, to, temp_file)

Parameters:

- schema: schema name.
- table: table name.
- part: partition name in schema name.
- field : field partition should have.
- from : field >= from value.
- to : field < to value.
- temp_file : temp file name to be used.
 
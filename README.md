NAME
====

Melian - Async Raku client for the Melian cache server

SYNOPSIS
========

```raku
use Melian;
my $melian = Melian.new(dsn => 'unix:///tmp/melian.sock');

# Fast
my %row = await $client.fetch-by-int-from( 'cats', 'id', 5);
my %row = await $client.fetch-by-string-from( 'cats', 'name', 'Pixel' );

# Faster, using IDs for the tables and columns
# (to discover the IDs, see schema() method)
my %row = await $client.fetch-by-int( 0, 0, 5 );
my %row = await $client.fetch-by-string( 1, 1, 'Pixel' );
```

DESCRIPTION
===========

Melian is a tiny, fast, no-nonsense Raku client for the Melian cache server.

[Melian](https://github.com/xsawyerx/melian/) (the server) keeps full table snapshots in memory. Lookups are done entirely inside the server and returned as small JSON blobs. Think of it as a super-fast read-only lookup service.

This module is a client to the Melian server, allowing fully asynchronous access to it in Raku.

SCHEMA
======

Melian needs a schema so it knows which table IDs and column IDs correspond to which names. A schema looks something like:

```
people#0|60|id#0:int
```

Or:

```
people#0|60|id#0:int,cats#1|45|id#0:int;name#1:string
```

The format is simple:

* `table_name#table_id` (multiple tables separated by `,`)
* `|refresh_period_in_seconds`
* `|column_name#column_id:column_type` (multiple columns separated by `;`)

You do NOT need to write this schema unless you want to. If you do not supply one, Melian will request it automatically from the server at startup.

If you provide a schema, it should match the schema set for the Melian server.

Accessing table and column IDs
------------------------------

Once the client is constructed:

```raku
my %schema = await $melian.schema;
```

Each table entry contains:

```
    {
        id      => 1,
        name    => "cats",
        period  => 45,
        indexes => [
            { id => 0, column => "id",   type => "int"    },
            { id => 1, column => "name", type => "string" },
        ],
    }
```

If you use the functional API, you probably want to store them in constants:

```raku
constant $CAT_ID_TABLE    = 1;
constant $CAT_ID_COLUMN   = 0; # integer column
constant $CAT_NAME_COLUMN = 1; # string column
```

This saves name lookups on every request.

METHODS
=======

`new(:schema, :schema-spec, :schema-file, :dsn)`
------------------------------------------------

```raku
my $melian = Melian.new(
    'dsn'         => 'unix:///tmp/melian.sock',
    'schema-spec' => 'people#0|60|id#0:int',
);
```

Creates a new client and automatically loads the schema.

You may specify:

* `schema` — already-parsed schema

```raku
my $melian = Melian.new(
    'schema' => %(
        'id'      => 1,
        'name'    => 'cats',
        'period'  => 45,
        'indexes' => [
            { 'id' => 0, 'column' => "id",   'type' => 'int'    },
            { 'id' => 1, 'column' => "name", 'type' => 'string' },
        ],
    ),
    ...
);
```

You would normally either provide a spec, a file, or nothing (to let Melian fetch it from the server).

* `schema-spec` — inline schema description

```raku
my $melian = Melian.new(
    'schema-spec' => 'cats#0|45|id#0:int;name#1:string',
    ...
);
```

* `schema-file` — path to JSON schema file

```raku
my $melian = Melian.new(
    'schema-file' => '/etc/melian/schema.json',
    ...
);
```

* nothing — Melian will ask the server for the schema

```raku
my $melian = Melian.new(...);
```

`fetch-raw(Int $table_id, Int $column_id, Buf:D $key)`
------------------------------------------------------

```raku
my buf8 $raw-key      = buf8.new(5, 0, 0, 0);
my buf8 $raw-response = await $client.fetch-raw(0, 0, $raw-key);
my %raw-row           = from-json($raw-response.decode('utf8'));
```

Fetches a raw JSON string. Does NOT decode it. Assumes input is encoded correctly.

You probably don't want to use this. See `fetch-by-int()`, `fetch-by-int-from()`, `fetch-by-string()`, and `fetch-by-string-from()` instead.

`fetch-raw-from(Str $table_name, Str $column_name, Buf:D $key)`
---------------------------------------------------------------

```raku
my buf8 $key          = encode-utf8-buf('Pixel');
my buf8 $raw-response = await $client.fetch-raw-from( 'cats', 'name', $key );
my %raw-row           = from-json( $raw-from-response.decode('utf8') );
```

Same as above, but uses names instead of table and column IDs.

You probably don't want to use this. See `fetch-by-int()`, `fetch-by-int-from()`, `fetch-by-string()`, and `fetch-by-string-from()` instead.

`fetch-by-string(Int $table_id, Int $column_id, Str:D $string_key)`
-------------------------------------------------------------------

```raku
my %cat = await $client.fetch-by-string( 1, 1, 'Pixel' );
```

Fetches JSON from the server and decodes into a Hash.

`fetch-by-string-from(Str $table_name, Str $column_name, Str:D $string_key)`
----------------------------------------------------------------------------

```raku
my %cat = await $client.fetch-by-string-from( 'cat', 'name', 'Pixel');
```

Name-based version. Slightly slower than using IDs.

`fetch-by-int(Int $table_id, Int $column_id, Int $int_key)`
-----------------------------------------------------------

```raku
my %row = await $client.fetch-by-int( 0, 0, 5 );
```

Same as `fetch-by-string`, but for integer-based column searches.

`fetch-by-int-from(Str $table_name, Str $column_name, Int $int_key)`
--------------------------------------------------------------------

```raku
my %row = await $client.fetch-by-int-from( 'cats', 'id', 5);
```

Name-based version. Slightly slower than using IDs.

PERFORMANCE NOTES
=================

* ID-based mode is faster because it skips name lookups.
* If you care about performance, use table and column IDs.


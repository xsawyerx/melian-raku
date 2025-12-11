unit class Melian;

use JSON::Fast;

constant HEADER-VERSION  = 0x11;
constant ACTION-FETCH    = 0x46; # 'F'
constant ACTION-DESCRIBE = 0x44; # 'D'

has Str     $.dsn     is rw = 'unix:///tmp/melian.sock';
has Numeric $.timeout is rw = 1; # TODO: use

has Promise $!schema-promise;
has Hash $!schema-input;
has Str  $!schema-spec is built;
has Str  $!schema-file is built;
has IO::Socket::Async $!socket;
has $!tap;
has buf8 $!read-buffer = buf8.new;
has @!read-waiters;
has Bool $!draining = False;

submethod BUILD( :$schema, :$schema-spec, :$schema-file, :$dsn, :$timeout ) {
    $!schema-input = $schema if $schema.defined;
    $!schema-spec  = $schema-spec if $schema-spec.defined;
    $!schema-file  = $schema-file if $schema-file.defined;
    $!dsn          = $dsn if $dsn.defined;
    $!timeout      = $timeout if $timeout.defined;
}

submethod TWEAK() {
    $!schema-promise = start {
        with $!schema-input {
            $_
        } orwith $!schema-spec {
            self!load-schema-from-spec($_);
        } orwith $!schema-file {
            self!load-schema-from-file($_);
        } else {
            # We want this to match the input from users
            my %struct := await self.describe-schema;
        }
    }
}

method schema(--> Promise) {
    $!schema-promise;
}

method describe-schema(--> Promise) {
    self!send(ACTION-DESCRIBE, 0, 0, buf8.new).then(-> $payload_p {
        my buf8 $payload = $payload_p.result;
        die 'Melian server returned empty schema description' unless $payload;
        my $decoded = from-json($payload.decode);
        die 'Schema description must be a JSON object' unless $decoded ~~ Hash;
        $decoded;
    });
}

method fetch-raw(Int $table-id, Int $index-id, Buf:D $key --> Promise) {
    self!validate-id($table-id, 'table-id');
    self!validate-id($index-id, 'index-id');
    self!send(ACTION-FETCH, $table-id, $index-id, $key);
}

method fetch-by-string(Int $table-id, Int $index-id, Str:D $key --> Promise) {
    my buf8 $encoded = buf8.new(|$key.encode('utf8').list);
    self.fetch-raw($table-id, $index-id, $encoded).then(-> $payload_p {
        my $payload = $payload_p.result;
        $payload.elems ?? self!decode-row($payload) !! Nil;
    });
}

method fetch-by-int(Int $table-id, Int $index-id, Int $record-id --> Promise) {
    my buf8 $key = self!encode-le32($record-id);
    self.fetch-raw($table-id, $index-id, $key).then(-> $payload_p {
        my $payload = $payload_p.result;
        $payload.elems ?? self!decode-row($payload) !! Nil;
    });
}

method fetch-by-string-from(Str $table-name, Str $column-name, Str:D $key --> Promise) {
    my ($table-id, $index-id) = await self.resolve-index($table-name, $column-name);
    self.fetch-by-string($table-id, $index-id, $key);
}

method fetch-by-int-from(Str $table-name, Str $column-name, Int $record-id --> Promise) {
    my ($table-id, $index-id) = await self.resolve-index($table-name, $column-name);
    self.fetch-by-int($table-id, $index-id, $record-id);
}

method fetch-raw-from(Str $table-name, Str $column-name, Buf:D $key --> Promise) {
    my ($table-id, $index-id) = await self.resolve-index($table-name, $column-name);
    self.fetch-raw($table-id, $index-id, $key);
}

method resolve-index(Str $table-name, Str $column-name --> Promise) {
    self.schema.then(-> $schema_p {
        my %schema = $schema_p.result;
        my $result;

        LOOP:
        for %schema<tables>.Array // [] -> $table {
            next unless $table<name> eq $table-name;

            for $table<indexes>.Array // [] -> $index {
                next unless $index<column> eq $column-name;
                $result = ($table<id>.Int, $index<id>.Int);
                last LOOP;
            }
        }

        die "Unable to resolve index for {$table-name}.{$column-name}"
            unless $result;

        $result;
    });
}

method !close() {
    self!handle-reader-closure('Client closed');
}

method !send(UInt $action, Int $table-id, Int $index-id, buf8 $payload --> Promise) {
    start {
        my $socket = await self!connect;
        my $message = self!build-message($action, $table-id, $index-id, $payload);
        my Int $bytes = $message.elems;

        my $written = await $socket.write($message);
        die "Did not write ($written) correct amount of bytes ($bytes)" if $written != $bytes;

        my buf8 $length-bytes = await self!read-exactly($socket, 4);
        my UInt $length = self!decode-be32($length-bytes);

        $length
            ?? await self!read-exactly($socket, $length)
            !! buf8.new;
    }
}

method !build-message(UInt $action, Int $table-id, Int $index-id, buf8 $payload --> Buf) {
    Buf
        .write-uint8(0, HEADER-VERSION)
        .write-uint8(1, $action)
        .write-uint8(2, $table-id +& 0xFF)
        .write-uint8(3, $index-id +& 0xFF)
        .write-uint32(4, $payload.elems, BigEndian)
        ~ $payload;
}

method !connect(--> Promise) {
    if $!socket.defined {
        return Promise.kept($!socket);
    }

    my $connect = do {
        if $.dsn.starts-with('unix://') {
            my $path = $.dsn.substr('unix://'.chars);
            die 'UNIX DSN must include a socket path' unless $path.chars;
            IO::Socket::Async.connect-path($path);
        } elsif $.dsn.starts-with('tcp://') {
            my $remainder = $.dsn.substr('tcp://'.chars);
            my ($host, $port) = $remainder.split(':', 2);
            die 'TCP DSN must include host:port' unless $host.defined && $port.defined && $host.chars && $port.chars;
            IO::Socket::Async.connect($host, $port.Int);
        } else {
            die "Unsupported DSN {$.dsn}";
        }
    };

    $connect.then(-> $socket_p {
        $!socket = $socket_p.result;
        self!ensure-reader($!socket);
        $!socket;
    });
}

method !read-exactly(IO::Socket::Async $socket, Int $bytes --> Promise) {
    self!ensure-reader($socket);
    my $promise = Promise.new;
    @!read-waiters.push({ bytes => $bytes, promise => $promise });
    self!drain-read-waiters;
    $promise;
}

method !ensure-reader(IO::Socket::Async $socket) {
    return if $!tap.defined;
    $!tap = $socket.Supply(:bin).tap(
        -> $chunk {
            $!read-buffer ~= $chunk;
            self!drain-read-waiters;
        },
        done => {
            self!handle-reader-closure('Socket closed while reading response');
        },
        quit => -> $err {
            self!handle-reader-closure($err // 'Socket error while reading response');
        }
    );
}

method !drain-read-waiters {
    return if $!draining;
    $!draining = True;
    while @!read-waiters {
        my %waiter = @!read-waiters[0];
        last unless %waiter ~~ Hash;
        my Int $need = %waiter<bytes>;
        last if $!read-buffer.elems < $need;
        @!read-waiters.shift;
        my buf8 $chunk = $!read-buffer.subbuf(0, $need);
        my buf8 $rest = $!read-buffer.subbuf($need);
        $!read-buffer = buf8.new(|$rest.list);
        %waiter<promise>.keep($chunk);
    }
    $!draining = False;
}

method !handle-reader-closure(Str $reason) {
    if $!tap.defined {
        my $tap = $!tap;
        $!tap = Nil;
        $tap.close;
    }
    if $!socket.defined {
        my $socket = $!socket;
        $!socket = Nil;
        $socket.close;
    }
    self!fail-readers($reason);
    $!read-buffer = buf8.new;
}

method !fail-readers(Str $reason) {
    for @!read-waiters -> %waiter {
        %waiter<promise>.break($reason);
    }
    @!read-waiters = ();
}

method !load-schema-from-file(Str $path --> Hash) {
    die "Schema file {$path} does not exist" unless $path.IO.f;
    my $text = slurp $path, :enc<utf8>;
    my $decoded = from-json($text);
    die 'Schema JSON file must decode to a Hash' unless $decoded ~~ Hash;
    $decoded;
}

method !load-schema-from-spec(Str $spec --> Hash) {
    my @tables;
    for $spec.split(',').map(*.trim) -> $chunk {
        next unless $chunk.chars;
        my @parts = $chunk.split('|', 3);
        die "Invalid table spec chunk {$chunk}" unless @parts.elems >= 1;
        my Str $table-part = @parts[0];
        my Int $period = @parts[1]:exists && @parts[1].chars ?? @parts[1].Int !! 0;
        my Str $columns-part = @parts[2] // '';
        my ($table-name, $table-id) = self!split-with-hash($table-part, 'table');
        die "Table {$table-name} must define at least one index" unless $columns-part.chars;
        my @indexes;
        for $columns-part.split(';').map(*.trim) -> $column-spec {
            next unless $column-spec.chars;
            my ($column-with-id, $type) = $column-spec.split(':', 2);
            $type //= 'int';
            my ($column-name, $column-id) = self!split-with-hash($column-with-id, 'index');
            @indexes.push({
                column => $column-name,
                id     => $column-id.Int,
                type   => $type,
            });
        }
        die "Table {$table-name} missing indexes" unless @indexes.elems;
        @tables.push({
            name   => $table-name,
            id     => $table-id.Int,
            period => $period,
            indexes => @indexes,
        });
    }
    die 'schema-spec produced no tables' unless @tables.elems;
    { tables => @tables };
}

method !split-with-hash(Str $value, Str $label --> List) {
    my @parts = $value.split('#', 2);
    die "Missing # delimiter for {$label} specification: {$value}"
        unless @parts.elems == 2 && @parts[0].chars && @parts[1].chars;
    @parts[0].trim, @parts[1].trim;
}

method !validate-id(Int $value, Str $label) {
    die "{$label} must be between 0 and 255" unless 0 <= $value <= 255;
}

method !encode-le32(Int $value --> buf8) {
    buf8.new(
        $value +& 0xFF,
        ($value +> 8) +& 0xFF,
        ($value +> 16) +& 0xFF,
        ($value +> 24) +& 0xFF,
    );
}

method !decode-row(buf8 $payload --> Hash) {
    my $decoded = from-json($payload.decode('utf8'));
    die 'Expected JSON object from server' unless $decoded ~~ Hash;
    $decoded;
}

method !decode-be32(buf8 $bytes --> UInt) {
    ($bytes[0] +< 24)
        + ($bytes[1] +< 16)
        + ($bytes[2] +< 8)
        + $bytes[3];
}
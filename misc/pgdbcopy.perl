#!/usr/bin/env perl

use warnings;
use strict;
use v5.28;

use experimental 'signatures';

use Data::Dumper;
use DBI;
use Try::Tiny;

# Configuration for what this script will accept, if anything
my $cli_spec = [qw(
    original_db_host=s
    original_db_user=s
    original_db_pass=s
    original_db_name=s
    new_db_host=s
    new_db_user=s
    new_db_pass=s
    new_db_name=s
)];

# Return whatever main sent as an exit value, if nothing exit on 0
exit main(@ARGV);

sub main(@args) {
    # Fetch an apphelper object
    my $app = _app_helper->new($cli_spec,\@args);

    # Insert code here
    # Connect to the original database
    my $dbh1_host = $app->cli_arg('original_db_host');
    my $dbh1_user = $app->cli_arg('original_db_user');
    my $dbh1_pass = $app->cli_arg('original_db_pass');
    my $dbh1_name = $app->cli_arg('original_db_name');
    my $dbh_old = DBI->connect(
        "dbi:Pg:dbname=$dbh1_name;host=$dbh1_host",
        $dbh1_user,
        $dbh1_pass, {
            'AutoCommit'    =>  0,
            'RaiseError'    =>  1
        }
    );

    # Connect to the new database
    my $dbh2_host = $app->cli_arg('new_db_host');
    my $dbh2_user = $app->cli_arg('new_db_user');
    my $dbh2_pass = $app->cli_arg('new_db_pass');
    my $dbh2_name = $app->cli_arg('new_db_name');
    my $dbh_new = DBI->connect(
        "dbi:Pg:dbname=$dbh2_name;host=$dbh2_host",
        $dbh2_user,
        $dbh2_pass, {
            'AutoCommit'    =>  1,
            'RaiseError'    =>  1
        }
    );

    # Let the fun begin...
    my $schema;

    # Find all tablenames we are dealing with
    my @tables = do {
        my $query   =   "SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema'";
        my $handle  =   $dbh_old->prepare($query);
        if ( !defined $handle ) {
            say STDERR "Cannot prepare table-list statement: $DBI::errstr\n";
            return 1;
        }
        $handle->execute;
        my $aref = $handle->fetchall_arrayref();
        $handle->finish();
        map { $_->[0] } @{$aref}
    };

    # Foreach table, extract the field definition for each col
    foreach my $table_name (@tables) {
        my $query   =   "SELECT c.oid, n.nspname, c.relname FROM pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE c.relname OPERATOR(pg_catalog.~) '^($table_name)\$' AND pg_catalog.pg_table_is_visible(c.oid) ORDER BY 2, 3";
        my $handle  =   $dbh_old->prepare($query);
        if ( !defined $handle ) {
            say STDERR "Error!: $DBI::errstr\n";
            return 1;
        }
        $handle->execute;
        my $aref = $handle->fetchall_arrayref();

        # Keep a copy of this information
        $schema->{$table_name}->{'meta'}->{'oid'}       =   $aref->[0]->[0];
        $schema->{$table_name}->{'meta'}->{'nspname'}   =   $aref->[0]->[1];
        $schema->{$table_name}->{'meta'}->{'relname'}   =   $aref->[0]->[2];

        # Make the OID nicer to deal with
        my $oid     =   $aref->[0]->[0];

        # Now let's see how the cols in the tables are constructed
        $query      =   "SELECT a.attname, pg_catalog.format_type(a.atttypid, a.atttypmod), (SELECT pg_catalog.pg_get_expr(d.adbin, d.adrelid, true) FROM pg_catalog.pg_attrdef d WHERE d.adrelid = a.attrelid AND d.adnum = a.attnum AND a.atthasdef), a.attnotnull, NULL AS attcollation, ''::pg_catalog.char AS attidentity, ''::pg_catalog.char AS attgenerated FROM pg_catalog.pg_attribute a WHERE a.attrelid = '$oid' AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum";
        $handle     =   $dbh_old->prepare($query);
        if ( !defined $handle ) {
            say STDERR "Error!: $DBI::errstr\n";
            return 1;
        }
        $handle->execute;
        $aref = $handle->fetchall_arrayref();

        # Store the cols and relevent information in the 'schema'
        # Store the col names for use later
        foreach my $col (@{$aref}) {
            my $col_name    =   $col->[0];
            my $col_type    =   $col->[1];
            my $col_default =   $col->[2];
            my $col_null    =   $col->[3];

            # Save the col in the meta index for order purpose
            push @{ $schema->{$table_name}->{'col_order'} },$col_name;

            # Save the details
            $schema->{$table_name}->{'col'}->{$col_name} = {
                'type'      =>  $col->[1],
                'default'   =>  $col->[2],
                'null'      =>  $col->[3]
            }
        }

        # Extract any indexes the table may be using
        $query      =   "SELECT pg_catalog.pg_get_indexdef(i.indexrelid, 0, true) FROM pg_catalog.pg_class c, pg_catalog.pg_class c2, pg_catalog.pg_index i WHERE c.oid = '$oid' AND c.oid = i.indrelid AND i.indexrelid = c2.oid";
        $handle     =   $dbh_old->prepare($query);
        if ( !defined $handle ) {
            say STDERR "Error!: $DBI::errstr\n";
            return 1;
        }
        $handle->execute;
        $aref = $handle->fetchall_arrayref();

        # Store the index in index
        $schema->{$table_name}->{'index'} = [map { $_->[0] } @{$aref}];

        # Extract anything that might reference this (reverse FK)
        $query      =   "SELECT conrelid::pg_catalog.regclass AS ontable, pg_catalog.pg_get_constraintdef(oid, true) AS condef FROM pg_catalog.pg_constraint WHERE confrelid = ? AND contype = 'f' ORDER BY conname";
        $handle     =   $dbh_old->prepare($query);
        if ( !defined $handle ) {
            say STDERR "Error!: $DBI::errstr\n";
            return 1;
        }
        $handle->execute($oid);
        $aref = $handle->fetchall_arrayref();

        # Store the index in index
        $schema->{$table_name}->{'referenced_by'} = [map { [$_->[0],$_->[1]] } @{$aref}];

        # For speed store the number of referenced_by records
        $schema->{$table_name}->{'meta'}->{'ref_by_count'} = 
            scalar @{ $schema->{$table_name}->{'referenced_by'} };

        # Be nice and close the prepared statement
        $handle->finish();
    }

    # We should now have enough information to somewhat recreate the schema *gump*
    # say Dumper $schema;

    # We need to figure out the order, create a copy of tables
    # But make it ordered by the most referenced
    # Its likely we will actually put the references in after table creation
    # so this is probably useless, but lets keep it in, incase its ever 
    # useful.
    # Most referenced first as its more likely the other tables depend on it
    # being there
    my @create_table = reverse do {
        my @ordered;
        my @final;
        foreach my $table_name (@tables) {
            my $referenced_by = 
                $schema->{$table_name}->{'meta'}->{'ref_by_count'};
            push @{$ordered[$referenced_by]},$table_name;
        }
        foreach my $order_set (@ordered) {
            my $count = scalar @{$order_set};
            if ($count == 0) { next }
            push @final,@{$order_set};
        }
        @final
    };

    # Sort it by what it least referenced first
    say "Table creation order: ".join ',',@tables;

# CREATE TABLE table_name(
#    column1 datatype,
#    column2 datatype,
#    column3 datatype,
#    .....
#    columnN datatype,
#    PRIMARY KEY( one or more columns )
# );

    # A big loop once again
    foreach my $table_name (@create_table) {
        say "Working on: $table_name";
        say "Cols: ".join ',',keys %{$schema->{$table_name}->{'col'}};

        # Base SQL statement
        my $base_statement = "CREATE TABLE $table_name(";

        # Foreach col add its details to the query
        foreach my $col_name (keys %{$schema->{$table_name}->{'col'}}) {
            my $base        =   $schema->{$table_name}->{'col'}->{$col_name};
            my $col_type    =   $base->{'type'};
            my $col_default =   $base->{'default'};
            my $col_null    =   $base->{'null'};

            # Add in each col
            # Is it a serial/sequence?
            if (
                $col_default 
                && $col_default =~ m/^nextval.*$/ 
                && $col_type eq 'integer'
            ) {
                # Yes it is
                $base_statement .=  "$col_name SERIAL";
            } else {
                # Base name and type
                $base_statement .=  "$col_name $col_type";
            }

            # Does it have a regular default?
            if (
                $col_default 
                && $col_default !~ m/^nextval.*$/ 
            ) {
                # Yes it does
                $base_statement .=  " default $col_default";
            }

            # Is it null or not
            if ($col_null) { 
                $base_statement .=  " NOT NULL,";
            } else {
                $base_statement .=  ",";
            }
        }

        # Clean up the end of query
        $base_statement = substr($base_statement,0,length($base_statement)-1);
        $base_statement .= ");";

        # IT WORKS and has sequences! 9 of them!
        # You need the Primary Key and foreign key stuff (well ideally at least
        # the primary, can always do the FK stuff manually)

        say "Create statement: $base_statement";
        say STDERR $base_statement;

        my $handle  =   $dbh_new->do($base_statement);
        if ( !defined $handle ) {
            say STDERR "Cannot pcreat table!: $DBI::errstr\n";
            return 1;
        }

        # Add in any indexs if there any any
        foreach my $index (@{ $schema->{$table_name}->{'index'} }) {
            say STDERR "$index;";
            $handle  =   $dbh_new->do("$index;");
            if ( !defined $handle ) {
                say STDERR "Cannot pcreat table!: $DBI::errstr\n";
                return 1;
            }
        }
    }

    # Add in any foreign keys if there are any
    foreach my $table_name (@create_table) { 
        foreach my $index (@{ $schema->{$table_name}->{'referenced_by'} }) {
            # ALTER TABLE table_0 add constraint db_id_fkey foreign key(db_id) REFERENCES table_1(db_id) ON UPDATE CASCADE ON DELETE CASCADE;
            #   [
            #     'work_days',
            #     'FOREIGN KEY (username) REFERENCES users(username)'
            #   ]
            #    TABLE "work_logger" CONSTRAINT "$1" FOREIGN KEY (work_day_id) REFERENCES work_days(work_day_id)
            my $target_table = $index->[0];
            my @fk_split = split(/\s+/,$index->[1]);
            my ($fk_local_vol) = $fk_split[2] =~ m/^\((.*)\)$/;
            my ($fk_table,$fk_col) = $fk_split[4] =~ m/^([a-z0-9_]+)\(([a-z0-9_]+)\)$/i;
            my $fk_ref = join('_',$fk_table,$fk_col);
            my $fk_ref_name = join('_',$fk_table,$fk_col,'seq');

            # This does not always work, lets hope for the best!
            my $query = "ALTER TABLE $target_table add constraint $fk_ref_name foreign key($fk_local_vol) REFERENCES $fk_table($fk_col) ON UPDATE CASCADE ON DELETE CASCADE;";
            say STDERR $query;

            my $handle  =   $dbh_new->do($query);
            if ( !defined $handle ) {
                say STDERR "Cannot pcreat table!: $DBI::errstr\n";
                return 1;
            }
        }
    }

    # begin selecting from the old tables and inserting them to the new
    # when a table is succesfully inserted we remove it from the array
    # this is a brute force way to get around the fk's
    my $sfety_loop = 10;
    my $blah = {};
    MAIN: while (scalar(@create_table) > 0 && $sfety_loop-- > 0)  {
        my $target_table;
        TABLE: foreach my $table_name (@create_table) {
            if ($blah->{$table_name}) { 
                next TABLE;
            }

            my $col_names       = 
                join(',',@{ $schema->{$table_name}->{'col_order'} });

            my $query   =   "SELECT $col_names FROM $table_name";
            my $handle  =   $dbh_old->prepare($query);
            if ( !defined $handle ) {
                say STDERR "Cannot prepare table-list statement: $DBI::errstr\n";
                return 1;
            }
            $handle->execute;
            # my $ keys %{$schema->{$table_name}->{'col'}}
            my $instant_insert_buffer = $handle->fetchall_arrayref();

            # Buffer for prepared statements
            my $prepared_insert;

            # Build the inserts
            foreach my $single_row (@{$instant_insert_buffer}) {
                # Optimization
                if (!$prepared_insert) {
                    my $col_count = scalar @{$single_row};
                    my $col_mask;

                    foreach my $col_val (@{$single_row}) {
                        $col_mask .= '?,';
                    }

                    # Remove the tailing ,
                    $col_mask = substr($col_mask,0,length($col_mask)-1);

                    my $insert_query    = 
                        "INSERT INTO $table_name($col_names) VALUES ($col_mask)";

                    $prepared_insert = $dbh_new->prepare($insert_query);
                }

                my $next_table = 0;
                try {
                    my $result = $prepared_insert->execute(@{$single_row});
                    if ($result) { 
                        if (!$blah->{$table_name}) {
                            $blah->{$table_name}++;
                            say STDERR "Success inserting to $table_name";
                        }
                    }
                    else {
                        say STDERR "TryCatch(SoftFail): $_";
                    }
                } catch {
                    if ($_ =~ m/violates foreign key constraint/) {
                        $next_table++;
                    }
                    else {
                        say STDERR "TryCatch(HardFail): $_";
                    }
                };
                if ($next_table) { next TABLE }
            }

            $handle->finish();
        }

        # Here we would look at inserted and remove it from create_table
    }

    # Close the handles cleanly
    $dbh_new->disconnect();
    $dbh_old->disconnect();

    return 0;
}


package _app_helper;

use warnings;
use strict;
use v5.28;

use experimental 'signatures';

use Getopt::Long;

sub new($class,$cli_spec,$args) {
    # Some private stuff for ourself
    my $self = {
        'cli'   =>  {
            'spec'  =>  $cli_spec,
            'args'  =>  $args
        }
    };

    # Go with god my son
    bless $self, $class;

    # use GetOpt to process the commandline
    $self->{'cli'}->{'opt'}     = 
        $self->process_cli;

    return $self;
}

sub set_defaults($self) {
    # set STDOUT hot
    $|=1;
    # TODO: if windows adjust binmode
}

sub process_cli($self) {
    my @args            =
        @{$self->{'cli'}->{'args'}};
    my %cli_values      =
        ();
    my $cli_parser      =
        Getopt::Long::Parser->new;
    my $cli_parse_state =
        $cli_parser->getoptionsfromarray(
            \@args,
            \%cli_values,
            @{$self->{'cli'}->{'spec'}}
        );

    { values=>\%cli_values, error=>$cli_parse_state?0:1 }
}
sub cli_arg($self,$key) {
    if (defined $self->{'cli'}->{'opt'}->{'values'}->{$key}) {
        return $self->{'cli'}->{'opt'}->{'values'}->{$key};
    }
    return undef;
}

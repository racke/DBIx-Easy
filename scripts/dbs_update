#! /usr/bin/perl -w

# Copyright (C) 1999 Stefan Hornburg

# Author: Stefan Hornburg <racke@linuxia.de>
# Maintainer: Stefan Hornburg <racke@linuxia.de>
# Version: 0.000_04

# This file is free software; you can redistribute it and/or modify it
# under the terms of the GNU General Public License as published by the
# Free Software Foundation; either version 2, or (at your option) any
# later version.

# This file is distributed in the hope that it will be
# useful, but WITHOUT ANY WARRANTY; without even the implied warranty
# of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this file; see the file COPYING.  If not, write to the Free
# Software Foundation, 675 Mass Ave, Cambridge, MA 02139, USA.

use strict;
use DBIx::CGI;
use Getopt::Long;

# process commandline parameters
my %opts;
my $whandler = $SIG{__WARN__};
$SIG{__WARN__} = sub {print STDERR "$0: @_";};
unless (GetOptions (\%opts, 'headline|h', 'routine|r=s', 'table|t=s')) {
  exit 1;
}
$SIG{__WARN__} = $whandler;

# sanity checks
if ($opts{'headline'}) {
    unless ($opts{'table'}) {
        die ("$0: missing table name\n");
    }
}

if ($#ARGV < 1) {
    die ("$0: need database driver and database name\n");
}

my %fieldmap;

my ($sth, $keyfield, $update);
my ($table, $key, $fieldnames, @values, $headline);
my (@columns, $routine);

if ($opts{'headline'}) {
    # the first row consists of the column names
    unless (defined ($headline = <STDIN>)) {
        die ("$0: empty input file\n");
    }
    my @columns = split /\t/, $headline;
    
    # fixed table name 
    $table = $opts{'table'};
    $fieldmap{$table} = \@columns;
}

if ($opts{'routine'}) {
    # read Perl subroutine for filtering the input
    $routine = eval $opts{'routine'};

    if ($@) {
        die "$0: invalid filter routine: $@: \n";
    }

    if (ref($routine) ne 'CODE') {
        die "$0: invalid filter routine\n";
    }
}

my $dbif = new DBIx::CGI (undef, @ARGV);
$dbif -> install_handler (\&fatal);

while (<STDIN>) {
    my (@data);

    # skip empty/blank/comment lines
    next if /^\#/; next if /^\s*$/;
    # remove newlines
    chomp;

    if ($opts{'headline'}) {
        # table name already known
        @values = split /\t/;
    } else {
        # table name is the first column
        ($table, @values) = split /\t/;

        # sanity check on the table name
        if ($table =~ /\s/) {
            warn ("$0: $.: skipping record (\"$table\" not accepted as table name)\n");
            next;
        }
    }

    if ($opts{'routine'}) {
        # filter input first
        filter_input ($routine, $table, $fieldnames, \@values);
    }
    
    $key = $values[0];
#    print join ('.', @values), "\n";

    # get key for table
    if (exists $fieldmap{$table}) {
        $fieldnames = $fieldmap{$table};
    } else {
        $sth = $dbif -> process ("SELECT * FROM $table WHERE 0 = 1");
        $fieldnames = $fieldmap{$table} = $sth -> {NAME};
        $sth -> finish ();
    }

    # check if record exists
    $sth = $dbif -> process ("SELECT $$fieldnames[0] FROM $table WHERE $$fieldnames[0] = "
                             . $dbif -> quote ($key));
    while ($sth -> fetch) {}

    if ($sth -> rows () > 1) {
        die ("$0: duplicate key $key in table $table\n");
    }

    $update = $sth -> rows ();

    # generate SQL statement
    for (my $i = 0; $i <= $#$fieldnames; $i++) {
        push (@data, $$fieldnames[$i], $values[$i]);
    }

    if ($update) {
#        print "UPDATING $.\n";
        $dbif -> update ($table, "$$fieldnames[0] = $key", @data);
    } else {
#        print "INSERTING $.\n";
        $dbif -> insert ($table, @data);
    }
}

# ---------------------------------------------------------
# FUNCTION: filter_input ROUTINE TABLE FIELDNAMES VALREF
#
# Filters data input with ROUTINE. Produces first a mapping
# between FIELDNAMES and the data pointed to by VALREF
# and passes the table name TABLE and the mapping to the
# ROUTINE.
# ---------------------------------------------------------

sub filter_input {
    my ($routine, $table, $fieldnames, $valref) = @_;
    my %colmap;

    # produce mapping
    for (my $i = 0; $i <= $#$fieldnames; $i++) {
        $colmap{$$fieldnames[$i]} = $$valref[$i];
    }

    # apply filter routine
    &$routine ($table, \%colmap);

    # write new values
    for (my $i = 0; $i <= $#$fieldnames; $i++) {
        $$valref[$i] = $colmap{$$fieldnames[$i]};
    }
    
}

sub fatal {
    my ($statement, $err, $msg) = @_;

    $sth -> finish if $sth;
    die ("$0: Statement \"$statement\" failed (ERRNO: $err, ERRMSG: $msg)\n");
}

# script documentation (POD style)

=head1 NAME

updatedb.pl - Update SQL Databases

=head1 DESCRIPTION

updatedb is an utility to update SQL databases from text files.

=head2 FORMAT OF THE TEXT FILES

updatedb assumes that each line of the input contains a data record
and that the field within the records are separated by tabulators.
The first field of the data record is used as table name.

Alternatively updatedb can read the column names from the first line
of input (see the B<-h>/B<--headline> option).

=head1 OPTIONS

=head2 -h, --headline

Reads the column names from the first line of the input instead
of dedicting them from the database layout. Requires the
B<-t/--table> option.

=head2 -t TABLE, --table=TABLE

Uses TABLE as table name for all records instead of the
first field name.

=head2 -r ROUTINE, --routine=ROUTINE

Applies ROUTINE to any data record. ROUTINE must be a subroutine.
updatedb passes the table name and a hash reference to this subroutine.
The keys of the hash are the column names and the values are the
corresponding field values.

=head1 AUTHOR

Stefan Hornburg, racke@linuxia.net

=head1 SEE ALSO

perl(1), DBIx::CGI(3)

=cut    

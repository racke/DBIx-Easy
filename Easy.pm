# Easy.pm - Easy to Use DBI interface

# Copyright (C) 1999 Stefan Hornburg, Dennis Schön

# Authors: Stefan Hornburg <racke@linuxia.net>
#          Dennis Schön <dschoen@rio.gt.owl.de>
# Maintainer: Stefan Hornburg <racke@linuxia.net>
# Version: 0.06

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

package DBIx::Easy;

use strict;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

require Exporter;

@ISA = qw(Exporter);
# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
@EXPORT = qw(
);
$VERSION = '0.06';

use DBI;

=head1 NAME

DBIx::Easy - Easy to Use DBI interface

=head1 SYNOPSIS

  use DBIx::Easy;
  my $dbi_interface = new DBIx::Easy qw(Pg template1);

  $dbi_interface -> insert ('transaction',
                   id => serial ('transaction', 'transactionid'),
                   time => \$dbi_interface -> now);

  $dbi_interface -> update ('components', "table='ram'", price => 100);
  $dbi_interface -> makemap ('components', 'id', 'price');
  $components = $dbi_interface -> rows ('components');
  $components_needed = $dbi_interface -> rows ('components', 'stock = 0');

=head1 DESCRIPTION

DBIx::Easy is an easy to use DBI interface.
Currently only the Pg, mSQL and mysql drivers are supported.

=head1 CREATING A NEW DBI INTERFACE OBJECT

  $dbi_interface = new DBIx::Easy qw(Pg template1);
  $dbi_interface = new DBIx::Easy qw(Pg template1 racke);
  $dbi_interface = new DBIx::Easy qw(Pg template1 racke aF3xD4_i);
  $dbi_interface = new DBIx::Easy qw(Pg template1 racke@linuxia.net aF3xD4_i);

The required parameters are the database driver
and the database name. Additional parameters are the database user
and the password to access the database. To specify the database host
use the USER@HOST notation for the user parameter.

=head1 ERROR HANDLING

  sub fatal {
    my ($statement, $err, $msg) = @_;
    die ("$0: Statement \"$statement\" failed (ERRNO: $err, ERRMSG: $msg)\n");
  }
  $dbi_interface -> install_handler (\&fatal);

If any of the DBI methods fails, either I<die> will be invoked
or an error handler installed with I<install_handler> will be
called.

=cut

# Variables
# =========

my $maintainer_adr = 'racke@linuxia.net';

# Keywords for connect()
my %kwmap = (mSQL => 'database', mysql => 'database', Pg => 'dbname');
my %kwhostmap = (mSQL => 'host', mysql => 'host', Pg => 'host');

# Whether the DBMS supports transactions
my %transactmap = (mSQL => 0, mysql => 0, Pg => 1);
  
# Statement generators for serial()
my %serialstatmap = (mSQL => sub {"SELECT _seq FROM $_[0]";},
					 Pg => sub {"SELECT NEXTVAL ('$_[1]')";});

# Statement for obtaining the table structure
my %obtstatmap = (mSQL => sub {my $table = shift;
							   "SELECT " . join (', ', @_)
                                 . " FROM $table WHERE 0 = 1";},
				  mysql => sub {my $table = shift;
							   "SELECT " . join (', ', @_)
                                 . " FROM $table WHERE 0 = 1";},
				  Pg => sub {my $table = shift;
							 "SELECT " . join (', ', @_)
							   . " FROM $table WHERE FALSE";});
  
# Supported functions
my %funcmap = (mSQL => {COUNT => 0},
			   mysql => {COUNT => 1},
			   Pg => {COUNT => 1});

# Preloaded methods go here.

sub new
  {
	my $proto = shift;
	my $class = ref ($proto) || $proto;
	my $self = {};

	$self ->{DRIVER} = shift;
	$self ->{DATABASE} = shift;
	$self ->{USER} = shift;
	# check for a host part
	if (defined $self->{USER} && $self->{USER} =~ /@/) {
	  $self->{HOST} = $';
	  $self->{USER} = $`;
	}
    $self ->{PASS} = shift;
	$self ->{CONN} = undef;
	$self ->{HANDLER} = undef;		# error handler

	bless ($self, $class);
	
    # sanity checks    
    unless (defined ($self -> {DRIVER}) && $self->{DRIVER} =~ /\S/) {
      $self -> fatal ("No driver selected for $class.");
    }
    unless (defined ($self -> {DATABASE}) && $self->{DATABASE} =~ /\S/) {
      $self -> fatal ("No database selected for $class.");
    }

    # check if this driver is supported
	unless (exists $kwmap{$self -> {DRIVER}}) {
      $self -> fatal ("Sorry, $class doesn't support the \""
                      . $self -> {DRIVER} . "\" driver.\n" 
                      . "Please send mail to $maintainer_adr for more information.\n");
    }

    # we may try to get password from DBMS specific
    # configuration file

    unless (defined $self->{PASS}) {
        unless (defined $self->{'USER'}
            && $self->{'USER'} ne getpwuid($<)) {   
            $self->passwd();
        }
    }

	return ($self);
}

# ------------------------------------------------------
# DESTRUCTOR
#
# If called for an object with established connection we
# commit any changes.
# ------------------------------------------------------

sub DESTROY {
	my $self = shift;

	if (defined ($self -> {CONN})) {
        unless ($self -> {CONN} -> {AutoCommit}) {
            $self -> {CONN} -> commit;
        }
	    $self -> {CONN} -> disconnect;
    }
}

# ------------------------------
# METHOD: fatal
#
# Error handler for this module.
# ------------------------------

sub fatal
  {
	my ($self, $info, $err) = @_;
	my $errstr = '';

	if (defined $self -> {CONN})
	  {
		$err = $DBI::err;
		$errstr = $DBI::errstr;
		
		# something has gone wrong, rollback anything
		$self -> {CONN} -> rollback ();
	  }
    
	if (defined $self -> {'HANDLER'})
	  {
		&{$self -> {'HANDLER'}} ($info, $err, $errstr);
	  }
	elsif (defined $self -> {CONN})
	  {
		die "$info (DBERR: $err, DBMSG: $errstr)\n";
	  }
	else
	  {
		die "$info\n";
	  }
  }

# ---------------------------------------------------------------
# METHOD: connect
#
# Establishes the connection to the database if not already done.
# Returns database handle if successful, dies otherwise.
# ---------------------------------------------------------------

sub connect ()
  {
	my $self = shift;
	my ($dsn, $oldwarn);
	my $msg = '';
    
	unless (defined $self -> {CONN})
	  {
		# build the data source string for DBI
		# ... the driver name
		$dsn .= 'dbi:' . $self -> {DRIVER} . ':';
		# ... the database part
		$dsn .= $kwmap{$self -> {DRIVER}} . "=" . $self -> {DATABASE};
		# ... optionally the host part
		if ($self -> {HOST}) {
			$dsn .= ';' . $kwhostmap{$self->{DRIVER}}
				. '=' . $self -> {HOST};
		}

        # install warn() handler to catch DBI error messages
        $oldwarn = $SIG{__WARN__};
        $SIG{__WARN__} = sub {$msg = "@_";};
        
		$self -> {CONN} = DBI
            -> connect ($dsn, $self -> {USER}, $self -> {PASS},
                        {AutoCommit => !$transactmap{$self->{DRIVER}}});

        # deinstall warn() handler
        $SIG{__WARN__} = $oldwarn;
        
		unless (defined $self -> {CONN})
		  {
            # remove file/line information from error message
            $msg =~ s/\s+at .*?line \d+\s*$//;
            
			# print error message in any case
			$self -> fatal ("Connection to database \"" . $self -> {DATABASE}
			  . "\" couldn't be established", $msg);
            return;
		  }
	  }

	# no need to see SQL errors twice
	$self -> {CONN} -> {'PrintError'} = 0;
	$self -> {CONN};
  }

# -------------------------
# METHOD: process STATEMENT
# -------------------------

=head1 METHODS

=over 4

=item process I<statement>

  $sth = $dbi_interface -> process ("SELECT * FROM foo");
  print "Table foo contains ", $sth -> rows, " rows.\n";

Processes I<statement> by just combining the I<prepare> and I<execute>
steps of the DBI. Returns statement handle in case of success.

=back

=cut

sub process
  {
  my ($self, $statement) = @_;
  my ($sth, $rv);
  
  $self -> connect ();

  # prepare and execute it
  $sth = $self -> {CONN} -> prepare ($statement)
	|| $self -> fatal ("Couldn't prepare statement \"$statement\"");
  $rv = $sth -> execute ()
	|| $self -> fatal ("Couldn't execute statement \"$statement\"");

  $sth;
  }

# ------------------------------------------------------
# METHOD: insert TABLE COLUMN VALUE [COLUMN VALUE] ...
#
# Inserts the given COLUMN/VALUE pairs into TABLE.
# ------------------------------------------------------

=over 4

=item insert I<table> I<column> I<value> [I<column> I<value>] ...

  $sth = $dbi_interface -> insert ('bar', drink => 'Caipirinha');

Inserts the given I<column>/I<value> pairs into I<table>. Determines from the
SQL data type which values has to been quoted. Just pass a reference to
the value to protect values with SQL functions from quoting.

=back

=cut

sub insert ($$$;@)
  {
	my $self = shift;
	my $table = shift;
	my (@columns, @values);
	my ($statement, $sthtest, $flags);
	my ($column, $value);

	$self -> connect ();

	while ($#_ >= 0)
	  {
		$column = shift; $value = shift;
		push (@columns, $column);
		push (@values, $value);
	  }

	# get the table structure
	$sthtest = $self -> process
	  (&{$obtstatmap{$self -> {'DRIVER'}}} ($table, @columns));
	$flags = $sthtest -> {'TYPE'};
    $sthtest -> finish ();
    
	for (my $i = 0; $i <= $#values; $i++) {
        if (ref ($values[$i]) eq 'SCALAR') {
			$values[$i] = ${$values[$i]};
        }
        elsif (defined $values[$i]) {
			unless ($$flags[$i] == DBI::SQL_INTEGER) {
                $values[$i] = $self -> quote ($values[$i]);
            }
        }
        else {
            $values[$i] = 'NULL';
        }
    }
	
	# now the statement
	$statement = "INSERT INTO $table ("
	  . join (', ', @columns) . ") VALUES ("
		. join (', ', @values) . ")";

	# process it
	$self -> {CONN} -> do ($statement)
	  || $self -> fatal ("Couldn't execute statement \"$statement\"");
  }

# ---------------------------------------------------------------
# METHOD: update TABLE CONDITIONS COLUMN VALUE [COLUMN VALUE] ...
#
# Updates the rows matching CONDITIONS with the given
# COLUMN/VALUE pairs and returns the number of the
# modified rows.    
# ---------------------------------------------------------------

=over 4

=item update I<table> I<conditions> I<column> I<value> [I<column> I<value>] ...

  $dbi_interface -> update ('components', "table='ram'", price => 100);

Updates any row of I<table> which fulfill the I<conditions> by inserting the given I<column>/I<value> pairs. Returns the number of rows modified.

=back

=cut

sub update
  {
	my $self = shift;
	my $table = shift;
	my $conditions = shift;
	my (@columns);
	my ($statement, $rv);
	my ($column, $value);

	# ensure that connection is established
	$self -> connect ();
	
	while ($#_ >= 0)
	  {
		$column = shift; $value = shift;
        # avoid Perl warning
        if (defined $value) {
            $value = $self -> {CONN} -> quote ($value);
        } else {
            $value = 'NULL';
        }
		push (@columns, $column . ' = ' . $value);
	  }

	# now the statement
	$statement = "UPDATE $table SET "
	  . join (', ', @columns) . " WHERE $conditions";

	# process it
	$rv = $self -> {CONN} -> do ($statement);
    if (defined $rv) {
        # return the number of rows changed
        $rv;
    } else {
        $self -> fatal ("Couldn't execute statement \"$statement\"");
    }
}

# -------------------------------
# METHOD: rows TABLE [CONDITIONS]
# -------------------------------

=over 4

=item rows I<table> [I<conditions>]

  $components = $dbi_interface -> rows ('components');
  $components_needed = $dbi_interface -> rows ('components', 'stock = 0');

Returns the number of rows within I<table> satisfying I<conditions> if any.

=back

=cut

sub rows
  {
	my $self = shift;
	my ($table, $conditions) = @_;
	my ($sth, $aref, $rows);
	my $where = '';
	
	if (defined ($conditions))
	  {
		$where = " WHERE $conditions";
	  }
	
	# use COUNT(*) if available
	if ($funcmap{$self -> {DRIVER}}->{COUNT})
	  {
		$sth = $self -> process ("SELECT COUNT(*) FROM $table$where");
		$aref = $sth->fetch;
		$rows = $$aref[0];
	  }
	else
	  {
		$sth = $self -> process ("SELECT * FROM $table$where");
		$rows = $sth -> rows;
	  }

	$rows;
  }

# ---------------------------------------------
# METHOD: makemap TABLE KEYCOL VALCOL
# ---------------------------------------------

=over 4

=item makemap I<table> I<keycol> I<valcol>

    $dbi_interface -> makemap ('components', 'id', 'price');
    
Produces a mapping between the values within column
I<keycol> and column I<valcol> from I<table>.

=back

=cut

sub makemap {
    my ($self, $table, $keycol, $valcol) = @_;
    my ($sth, $row, %map);

    # read all rows from the specified table
    $sth = $self -> process ("SELECT $keycol, $valcol FROM $table");

    while ($row = $sth -> fetch) {
        $map{$$row[0]} = $$row[1];
    }

    \%map;
}

# -------------------------------  
# METHOD: serial TABLE SEQUENCE
# -------------------------------

=over 4

=item serial I<table> I<sequence>

Returns a serial number for I<table> by querying the next value from
I<sequence>. Depending on the DBMS one of the parameters is ignored.
This is I<sequence> for mSQL resp. I<table> for PostgreSQL. mysql
doesn't support sequences, but the AUTO_INCREMENT keyword for fields.
In this case this method returns 0 and mysql generates a serial
number for this field.

=back

=cut
#'

sub serial 
  {
	my $self = shift;
	my ($table, $sequence) = @_;
	my ($statement, $sth, $rv, $resref);
	
	$self -> connect ();
    return ('0') if $self->{DRIVER} eq 'mysql';

	# get the appropriate statement
	$statement = &{$serialstatmap{$self->{DRIVER}}};

	# prepare and execute it
	$sth = $self -> process ($statement);

	unless (defined ($resref = $sth -> fetch))
	  {
		$self -> fatal ("Unexpected result for statement \"$statement\"");
	  }

	$$resref[0];
  }

# ---------------------------------------------------------
# METHOD: fill STH HASHREF [FLAG COLUMN ...]
#
# Fetches the next table row from the result stored in STH.
# ---------------------------------------------------------

=over 4

=item fill I<sth> I<hashref> [I<flag> I<column> ...]

Fetches the next table row from the result stored into I<sth>
and records the value of each field in I<hashref>. If I<flag>
is set, only the fields specified by the I<column> arguments are
considered, otherwise the fields specified by the I<column> arguments
are omitted.

=back

=cut

sub fill
  {
	my ($dbi_interface, $sth, $hashref, $flag, @columns) = @_;
	my ($fetchref);

	$fetchref = $sth -> fetchrow_hashref;
	if ($flag)
	  {
		foreach my $col (@columns)
		  {
			$$hashref{$col} = $$fetchref{$col};
		  }
	  }
	else
	  {
		foreach my $col (@columns)
		  {
			delete $$fetchref{$col};
		  }
		foreach my $col (keys %$fetchref)
		  {
			$$hashref{$col} = $$fetchref{$col};
		  }
	  }
  }

# ------------------------------------------------------
# METHOD: view TABLE
#
# Produces text representation for database table TABLE.
# ------------------------------------------------------

=over 4

=item view I<table> [I<name> I<value> ...]

  foreach my $table (sort $dbi_interface -> tables)
    {
    print $cgi -> h2 ('Contents of ', $cgi -> code ($table));
    print $dbi_interface -> view ($table);
    }

Produces plain text representation of the database table
I<table>. This method accepts the following options as I<name>/I<value>
pairs:

B<order>: Which column to sort the row after.

B<limit>: Maximum number of rows to display.

B<where>: Display only rows matching this condition.

  print $dbi_interface -> view ($table,
                                order => $cgi -> param ('order') || '',
                                where => "price > 0");

=back

=cut

sub view
  {
    my ($self, $table, %options) = @_;
    my ($view, $sth);
    my ($orderstr, $condstr) = ('', '');
    
    # anonymous function for cells in top row
    # get contents of the table
    if ((exists ($options{'order'}) && $options{'order'})) {
      $orderstr = " ORDER BY $options{'order'}";
    }
    if ((exists ($options{'where'}) && $options{'where'})) {
      $condstr = " WHERE $options{'where'}";
    } 
    $sth = $self -> process ("SELECT * FROM $table$condstr$orderstr");
    my $names = $sth -> {NAME};
    $view = join(" | ", map {$_} @$names) . "\n";
    my $count;
    while((my $ref = $sth->fetch) && ($count != $options{'limit'})) {
      $count++;
      $view .= join("| ", map {$_} @$ref) . "\n";
    }
    my $rows = $sth -> rows;
    $view .="($rows rows)";
    $view;
  }

# --------------------------------------------
# METHOD: now
#
# Returns representation for the current time.
# --------------------------------------------

=head1 TIME VALUES

=over 4

=item now

  $dbi_interface -> insert ('transaction',
                   id => serial ('transaction', 'transactionid'),
                   time => \$dbi_interface -> now);

Returns representation for the current time. Uses special values of
the DBMS if possible.

=back

=cut

sub now
  {
	my $self = shift;

    # Postgres and mysql have an special value for the current time
    return 'now' if $self -> {'DRIVER'} eq 'Pg';
	return 'now()' if $self -> {'DRIVER'} eq 'mysql';
    # determine current time by yourself
	scalar (gmtime ());
  }

# --------------------------------------------------
# METHOD: money2num MONEY
#
# Converts the monetary value MONEY (e.g. $10.00) to
# a numeric one.
# --------------------------------------------------

=head1 MONETARY VALUES

=over 4

=item money2num I<money>

Converts the monetary value I<money> to a numeric one.

=back

=cut

sub money2num
  {
	my ($self, $money) = @_;

	# strip leading dollar sign
	$money =~ s/\$//;
	# remove commas
	$money =~ s/,//g;
	# ignore empty pennies
	$money =~ s/\.00$//;
	
	$money;
  }

# METHOD: passwd

sub passwd {
    my ($self) = shift;
    my ($mycnf) = $ENV{'HOME'} . "/.my.cnf";
    my $clientsec = 0;
    my ($option, $value);
    
    # implemented only for mysql
    return unless $self->{'DRIVER'} eq 'mysql';
    
    # just give up if file is not accessible
    open (CNF, $mycnf) || return;
    while (<CNF>) {
        # ignore comments and blank lines
        next if /^\#/ or /^;/;
        next unless /\S/;
        # section ?
        if (/\[(.*?)\]/) {
            if (lc($1) eq 'client') {
                $clientsec = 1;
            } else {
                $clientsec = 0;
            }
        } elsif ($clientsec) {
            # in the [client] section check for password option
            ($option, $value) = split (/=/, $_, 2);
            if ($option =~ /\s*password\s*/) {
                $value =~ s/^\s+//;
                $value =~ s/\s+$//;
                $self->{'PASS'} = $value;
                last;
            }
        }
    }
        
    close (CNF);
}

# install error handler
sub install_handler {$_[0] -> {'HANDLER'} = $_[1];}

# direct interface to DBI
sub prepare {my $self = shift; $self -> prepare (@_);}
sub commit {$_[0] -> connect () -> commit ();}
sub quote {$_[0] -> connect () -> quote ($_[1]);}

sub tables
  {
  my $self = shift;

  # mSQL/mysql doesn't support DBI method tables yet
  if ($self -> {DRIVER} eq 'mSQL' || $self -> {DRIVER} eq 'mysql')
	{
	  $self -> connect () -> func('_ListTables');
	}
  else
	{
	  # standard method
	  $self -> connect () -> tables ();
	}
  }

1;
__END__

=head1 AUTHORS

Stefan Hornburg, racke@linuxia.net
Dennis Schön, dschoen@rio.gt.owl.de

=head1 SEE ALSO

perl(1), DBI(3), DBD::Pg(3), DBD::mysql(3), DBD::msql(3).

=cut

# Import.pm - Easy to Use DBI import interface

# Copyright (C) 2004 Stefan Hornburg (Racke) <racke@linuxia.de>

# Authors: Stefan Hornburg (Racke) <racke@linuxia.de>
# Maintainer: Stefan Hornburg (Racke) <racke@linuxia.de>
# Version: 0.16

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

package DBIx::Easy::Import;

use strict;
use vars qw($VERSION @ISA @EXPORT @EXPORT_OK);

require Exporter;

@ISA = qw(Exporter);
# Items to export into callers namespace by default. Note: do not export
# names by default without a very good reason. Use EXPORT_OK instead.
# Do not simply export all your public functions/methods/constants.
@EXPORT = qw(
);

# Public variables
$VERSION = '0.16';

use DBI;
use DBIx::Easy;

sub new {
	my $proto = shift;
	my $class = ref($proto) || $proto;
	my $self = {};

	bless ($self, $class);
}

sub update {
	my ($self, %params) = @_;

	$self->_do_import(%params);
}

sub _do_import {
	my ($self, %params) = @_;
	my ($format, $sep_char, %colmap, %hcolmap);
	
	if ($params{file}) {
		# read input from file
		require IO::File;
		$self->{fd_input} = new IO::File;
		$self->{fd_input}->open($params{file})
			|| die "$0: couldn't open $params{file}: $!\n";
	} else {
		# read input from standard input
		require IO::Handle;
		$self->{fd_input} = new IO::Handle;
		$self->{fd_input}->fdopen(fileno(STDIN),'r');
	}

	if ($params{'format'}) {
		$format = uc($params{'format'});

		if ($format =~ /^CSV/) {
			$format = 'CSV';
			if ($') {
				$sep_char = $';
				$sep_char =~ s/^\s+//;
				$sep_char =~ s/\s+$//;
			}
			eval {
				require Text::CSV_XS;
			};
			if ($@) {
				die "$0: couldn't load module Text::CSV_XS\n";
			}
			$self->{func} = \&get_columns_csv;
			$self->{parser} = new Text::CSV_XS ({'binary' => 1, 'sep_char' => $sep_char});
		} elsif ($format eq 'XLS') {
			eval {
				require Spreadsheet::ParseExcel;
			};
			if ($@) {
				die "$0: couldn't load module Spreadsheet::ParseExcel\n";
			}
			$self->{parser} = new Spreadsheet::ParseExcel;
		} else {
			die qq{$0: unknown format "$params{format}"}, "\n";
		}
	}

	my @columns;
	if ($self->{func}->($self, \@columns) <= 0)  {
		die "$0: couldn't find headline\n";
	}

	if ($params{'map_filter'} eq 'lc') {
		@columns = map {lc($_)} @columns;
	}
		
	# remove whitespace from column names and mark them
	map {s/^\s+//; s/\s+$//; $hcolmap{$_} = 1;} @columns;

	if ($params{'map'}) {
        my @newcolumns;
        
        # filter column names
        foreach (@columns) {
            if (exists $colmap{$_}) {
                push (@newcolumns, $colmap{$_});
				$hcolmap{$colmap{$_}} = 1;
            } else {
                push (@newcolumns, $_);
            }
        }
        @columns = @newcolumns;
    }

    # add any other columns explicitly selected
	my %usecol;
    for (sort (keys %usecol)) {
        next if $hcolmap{$_};
        next unless exists $usecol{$_};
        next unless $usecol{$_};
        push (@columns, $_);
    }

	# database access
	my $dbif = new DBIx::Easy ($self->{driver} || $params{driver},
							   $self->{database} || $params{database});

}

# FUNCTION: get_columns_csv IREF FD COLREF

sub get_columns_csv {
	my ($self, $colref) = @_;
	my $line;
	my $msg;
	my $fd = $self->{fd_input};
	
	while (defined ($line = <$fd>)) {
		if ($self->{parser}->parse($line)) {
			# csv line completed, delete buffer
			@$colref = $self->{parser}->fields();
			$self->{buffer} = '';
			return @$colref;
		} else {
			if (($line =~ tr/"/"/) % 2) {
			# odd number of quotes, try again with next line
				$self->{buffer} = $line;
			} else {
				$msg = "$0: $.: line not in CSV format: " . $self->{parser}->error_input() . "\n";
				die ($msg);
			}
		}
	}
}

# ----------------------------------------
# FUNCTION: get_columns_tab IREF FD COLREF
#
# Get columns from a tab separated file.
# ----------------------------------------

sub get_columns_tab {
	my ($iref, $fd, $colref) = @_;
	my $line;
	
	while (defined ($line = <$fd>)) {
		# skip empty/blank/comment lines
		next if $line =~ /^\#/; next if $line =~ /^\s*$/;
		# remove newlines and carriage returns
		chomp ($line);
		$line =~ s/\r$//;

		@$colref = split (/\t/, $line);
		return @$colref;
	}
}

# ----------------------------------------
# FUNCTION: get_columns_xls IREF FD COLREF
#
# Get columns from a XLS spreadsheet.
# ----------------------------------------

sub get_columns_xls {
	my ($iref, $fd, $colref) = @_;

	unless ($iref->{workbook}) {
		# parse the spreadsheet once
		$iref->{workbook} = $iref->{object}->Parse($fd);
		unless ($iref->{workbook}) {
			die "$0: couldn't parse spreadsheet\n";
		}
		$iref->{worksheet} = $iref->{workbook}->{Worksheet}[0];
		$iref->{row} = 0;
	}

	if ($iref->{row} <= $iref->{worksheet}->{MaxRow}) {
		@$colref = map {$_->Value()}
			@{$iref->{worksheet}->{Cells}[$iref->{row}++]};
		return @$colref;
	}
}

1;

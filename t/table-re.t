#!/usr/bin/env perl

use strict;
use warnings;
use Test::More;
use Data::Dumper;
# check the regexp

my @list = (
            '`first_table`',
            '`second_table`',
            '`test`.`first_table`',
            '`test`.`second_table`',
           );

my $dbname = 'test';

my @got = map {s/^(`\Q$dbname\E`\.)?`(.*)`$/$2/; $_} @list;

print Dumper(\@got);

is_deeply(\@got, [qw/first_table
                     second_table
                     first_table
                     second_table/]);
done_testing;

use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.

# List here scripts that should be installed 
my @scripts = ("./scripts/dbs_empty",
               "./scripts/dbs_update",
               "./scripts/dbs_printtab",
               "./scripts/dbs_dumptabdata",
               "./scripts/dbs_dumptabstruct");

WriteMakefile(
    'NAME'	=> 'DBIx::Easy',
    'VERSION_FROM' => 'Easy.pm', # finds $VERSION
    'INSTALLMAN3DIR' => '/usr/share/man/man3',
    'EXE_FILES' => \@scripts
);

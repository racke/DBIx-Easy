use ExtUtils::MakeMaker;
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
WriteMakefile(
    'NAME'	=> 'DBIx::Easy',
    'VERSION_FROM' => 'Easy.pm', # finds $VERSION
    'INSTALLMAN3DIR' => '/usr/man/man3'
);

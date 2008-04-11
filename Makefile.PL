use strict;
use inc::Module::Install;

name('DBIx-Replicate');
all_from('lib/DBIx/Replicate.pm');

requires 'Carp::Clan';
requires 'Class::Accessor::Fast';
requires 'Exporter', '5.60';
requires 'List::Util';
requires 'DBI';
requires 'UNIVERSAL::require';

auto_install;
WriteAll;

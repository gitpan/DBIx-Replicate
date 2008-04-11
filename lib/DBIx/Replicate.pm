# $Id: Replicate.pm 5801 2008-01-29 10:01:14Z daisuke $

package DBIx::Replicate;
use strict;
use warnings;
use Carp::Clan;
use DBI;
use DBIx::Replicate::Node;
use UNIVERSAL::require;
use Exporter 'import';
use base qw(Class::Accessor::Fast);

our %EXPORT_TAGS = (
    'all' => [ qw/dbix_replicate/ ],
);
our @EXPORT_OK = map { @{$EXPORT_TAGS{$_}} } qw/all/;
our $VERSION = '0.02';

__PACKAGE__->mk_accessors($_) for qw(src dest columns block extra_cond strategy);

sub new
{
    my $class = shift;
    my $args  = shift || {};

    if (! $args->{strategy}) {
        $args->{strategy_class} ||= 'DBIx::Replicate::Strategy::PK';
    }

    if ( my $strategy_class = $args->{strategy_class}) {
        my $strategy_args = $args->{strategy_args} || {};
        $strategy_class->require or die;
        $args->{strategy} = $strategy_class->new($strategy_args);
    }

    foreach my $p (qw/src dest columns strategy/) {
        croak "required parameter $p is missing\n"
            unless $args->{$p};
    }
    $args->{block} ||= 1000;

    my $self  = $class->SUPER::new({
        strategy   => $args->{strategy},
        columns    => $args->{columns},
        block      => $args->{block},
        src        => $args->{src},
        dest       => $args->{dest},
        extra_cond => $args->{extra_cond},
    });
        
    return $self;
}

sub dbix_replicate {
    my $args = shift;

    $args = { %$args };

    foreach my $p (qw/src_table src_conn dest_table dest_conn columns/) {
        croak "required parameter $p is missing\n"
            unless $args->{$p};
    }

    my $src = DBIx::Replicate::Node->new( {
        table => delete $args->{src_table},
        conn  => delete $args->{src_conn},
    } );
    my $dest = DBIx::Replicate::Node->new( {
        table => delete $args->{dest_table},
        conn  => delete $args->{dest_conn}
    });

    if (! $args->{strategy} && ! $args->{strategy_class}) {
        if ($args->{copy_by}) {
            $args->{strategy_class} ||= 'DBIx::Replicate::Strategy::CopyBy';
        } else {
            $args->{strategy_class} ||= 'DBIx::Replicate::Strategy::PK';
        }
    }

    my %args = (
        src            => $src,
        dest           => $dest,
        columns        => delete $args->{columns},
        block          => delete $args->{block},
        extra_cond     => delete $args->{extra_cond},
        strategy       => delete $args->{strategy},
        strategy_class => delete $args->{strategy_class},
        strategy_args  => delete $args->{strategy_args},
    );

    my $dr = DBIx::Replicate->new( \%args );
    $dr->replicate($args);
}

sub replicate
{
    my ($self, $args) = @_;

    $self->strategy->replicate( $self, $args );
}


1;
__END__
=head1 NAME

DBIx::Replicate - Synchornizes an SQL table to anther table

=head1 SYNOPSIS

  use DBIx::Replicate qw/dbix_replicate/;
  
  # incrementally copy table to other database (copy by each zipcode)
  dbix_replicate({
    src_conn     => $src_dbh,
    src_table    => 'tbl',
    dest_conn    => $dest_dbh,
    dest_table   => 'tbl',
    copy_by      => [ qw/zipcode/ ],
  });
  
  # incrementally extract (by every 1000 rows) people younger than 20 years old
  dbix_replicate({
    src_conn     => $dbh,
    src_table    => 'all_people',
    dst_conn     => $dbh,
    dest_table   => 'young_people',
    primary_keys => [ qw/id/ ],
    columns      => [ qw/id name age/ ],
    block        => 1000,
    extra_cond   => 'age<20',
  });


  # OO interface
  my $dr = DBIx::Replicate->new(
    src => DBIx::Replicate::Node->new(...)
    dest => DBIx::Replicate::Node->new(...)
    strategy => DBIx::Replicate::Strategy::PK->new()
  );
  $dr->replicate();
  
=head1 DESCRIPTION

...

=head1 AUTHOR

Kazuho Oku

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2008 Cybozu Labs, Inc.

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.6 or,
at your option, any later version of Perl 5 you may have available.

=cut

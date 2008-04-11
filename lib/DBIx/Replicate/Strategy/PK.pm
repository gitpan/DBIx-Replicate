# $Id: PK.pm 6287 2008-02-06 06:35:36Z kazuho $

package DBIx::Replicate::Strategy::PK;
use strict;
use warnings;
use Carp::Clan;
use List::Util qw/min/;
use Time::HiRes qw/time sleep/;

sub new { bless {}, shift }

sub replicate
{
    my $self = shift;
    my $c    = shift;
    my $args = shift || {};

    foreach my $p qw(primary_key) {
        croak(ref($self) . ": required parameter $p is missing\n")
            unless $args->{$p};
    }

    # XXX Refactor later;
    my @columns = @{ $c->columns };
    my $columns_str = join ',', @columns;
    my $extra_cond = $c->extra_cond ?  sprintf("and %s", $c->extra_cond) : '';
    my $sql;
    
    my $block      = $c->block;
    my $src_table  = $c->src->table;
    my $dest_table = $c->dest->table;
    my $src_conn   = $c->src->conn;
    my $dest_conn  = $c->dest->conn;

    my $pkey = ref $args->{primary_key}
        ? $args->{primary_key} : [ $args->{primary_key} ];

    my $order_by = $args->{order_by} || join(',', @$pkey);
    
    # copy by 'limit $block'
    my ($start_srcconn, $start_destconn) = qw/1 1/;
    while (1) {
        my $start = time;
        $sql = "select $columns_str from $src_table where $start_srcconn $extra_cond order by $order_by limit $block";
        my $rows = $src_conn->selectall_arrayref(
            $sql,
            { Slice => {} },
        ) or die $src_conn->errstr;
        last unless @$rows;
        $dest_conn->begin_work
            or die $dest_conn->errstr;
        my $next_srcconn =
            '(' . join(',', @$pkey) . ')>(' . join(
                ',',
                map {
                    $src_conn->quote($rows->[-1]->{$_})
                } @$pkey,
            ) . ')';
        my $next_destconn =
            '(' . join(',', @$pkey) . ')>(' . join(
                ',',
                map {
                    $dest_conn->quote($rows->[-1]->{$_})
                } @$pkey,
            ) . ')';
        $sql = "delete from $dest_table where $start_destconn and not $next_destconn";
        $dest_conn->do($sql)
            or die $dest_conn->errstr;
        $sql = "insert into $dest_table ($columns_str) values "
            . join(
                ',',
                map {
                    my $row = $_;
                    '(' . join(
                        ',',
                        map {
                            $dest_conn->quote($row->{$_})
                        } @columns
                    ) . ')'
                } @$rows,
            );

        $dest_conn->do($sql)
            or die $dest_conn->errstr;
        $dest_conn->commit
            or die $dest_conn->errstr;
        sleep(min(time - $start, 0) * (1 - $args->{load}) / $args->{load})
            if $args->{load};
        ($start_srcconn, $start_destconn)
            = ($next_srcconn, $next_destconn);
    }
    $sql = "delete from $dest_table where $start_destconn";
    $dest_conn->do($sql)
        or die $dest_conn->errstr;
}

1;


use Test::Base;
use Test::TCP;
use AnyEvent::Gearman::Client;

eval q{
        use Gearman::Worker;
        use Gearman::Server;
    };
if ($@) {
    plan skip_all
        => "Gearman::Worker and Gearman::Server are required to run this test";
}

plan tests => 3;

my $port = empty_port;

sub run_tests {
    my $client = AnyEvent::Gearman::Client->new(
        job_servers => ['127.0.0.1:' . $port],
        prefix      => 'prefix',
    );

    # test that coalesced jobs from the same client all have their callbacks
    # fired and have the same value returned

    my %success;
    my $cv = AnyEvent->condvar;

    # timeout if a job's callbacks are never called
    my $watchdog = AE::timer(1, 0, sub { $cv->send });

    # submit the same job several times; the 'sleep' worker sleeps for a brief time
    # to make sure the jobs won't complete until all of them are submitted.
    # TODO: fix sleep race condition. could be done by only having worker
    # function return once it receives a SIGUSR1 or something from this client.

    my $jobs = 3;
    $cv->begin(sub { $cv->send });
    for my $task (1 .. $jobs) {
        $cv->begin;
        $client->add_task(
            'sleep', 'foo',
            unique => '-',
            on_complete => sub {
                $success{$task} = $_[1];
                $cv->end;
            },
            on_fail => sub {
                $cv->end;
            },
        );
    }
    $cv->end;
    $cv->recv;
    undef $watchdog;

    # although the sleep worker returns an incremental value for each time it
    # actually runs, we expect the coalesced jobs all got the same result
    is($success{$_}, 1, "task $_ got coalesced value") for reverse 1 .. $jobs;
}

my $child = fork;
if (!defined $child) {
    die "fork failed: $!";
}
elsif ($child == 0) {
    my $server = Gearman::Server->new( port => $port );
    $server->start_worker("$^X t/danga_worker.pl -s 127.0.0.1:$port -p prefix");
    Danga::Socket->EventLoop;
}
else {
    END { kill 9, $child if $child }
}

sleep 1;

run_tests;

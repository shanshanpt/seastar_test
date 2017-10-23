#include "core/print.hh"
#include "core/reactor.hh"
#include "core/app-template.hh"
#include "core/future-util.hh"
#include "core/distributed.hh"
#include "core/semaphore.hh"
#include "core/future-util.hh"
#include <chrono>
#include <array>
#include <string>
#include <sys/time.h>
#include <iostream>
#include <cstdint>

using namespace std;
using namespace seastar;

static size_t buf_size = 128; //128;//1600;
static int buf_count = 10000;
static long total_size = buf_size * buf_count;

static std::string str_txbuf(buf_size, 'X');
static std::string ping(4, 'p');


// return time in ns
static uint64_t getCurrentTimeNS()
{
    struct timespec start_time;
    clock_gettime(CLOCK_MONOTONIC, &start_time);
    return (uint64_t)start_time.tv_sec * 1000000000UL + start_time.tv_nsec;
}


// client
class tcp_echo_client {
public:
    // 
    unsigned _duration {0};
    // connections count per core
    unsigned _conn_per_core {0};
    // request count per conn
    unsigned _reqs_per_conn {0};
    // save all sockets
    std::vector<connected_socket> _sockets;
    // wait all conn connected
    semaphore _conn_connected {0};
    // 
    semaphore _conn_finished {0};
    timer<> _run_timer;
    
    bool _timer_based { false };
    
    bool _timer_done {false};
    // total request count
    uint64_t _total_reqs {0};

public:
    tcp_echo_client(unsigned duration, unsigned total_conn, unsigned reqs_per_conn)
        : _duration(duration)
        , _conn_per_core(total_conn / smp::count)
        , _reqs_per_conn(reqs_per_conn)
        , _run_timer([this] { _timer_done = true; })
        , _timer_based(reqs_per_conn == 0)
    {
        //cout << "new client: d=" << _duration << "   conn_per_core=" <<  _conn_per_core << "   reqs_per_conn" << _reqs_per_conn << "   total_reqs=" <<  _total_reqs << endl;
    }

    // connection class
    class connection {
    public:
        // socket
        connected_socket _fd;
        // 
        input_stream<char> _in;
        // 
        output_stream<char> _out;
        // 
        tcp_echo_client& _echo_client;
        uint64_t _nr_done{0};

        uint64_t _total_cost_time {0};
        uint64_t _max_time {0};
        uint64_t _min_time {UINT64_MAX};

        // recv resp count
        uint64_t _total_count {0};
        // send req count
        uint64_t _total_sent {0};
        // for buf to send
        int index = 0;
        // buf for send data
        temporary_buffer<char> buffff;
    public:
        connection(connected_socket&& fd, tcp_echo_client& echo_client)
            : _fd(std::move(fd))
            , _in(_fd.input())
            , _out(_fd.output())
            , _echo_client(echo_client)
        {
            //cout << "new connection: total_count=" << _total_count << "   total_sent=" << _total_sent << "   index=" << index << endl;
        }

        ~connection() {
        }

        uint64_t nr_done() {
            return _nr_done;
        }
    };

    // 
    future<uint64_t> total_reqs() {
        print("Requests on cpu %2d: %ld\n", engine().cpu_id(), _total_reqs);
        return make_ready_future<uint64_t>(_total_reqs);
    }

    // 
    bool done(uint64_t nr_done) {
        // 
        if (_timer_based) {
            return _timer_done;
        } else {
            // 
            return nr_done >= _reqs_per_conn;
        }
    }

    // connect to server
    future<> connect(ipv4_addr server_addr) {
        // all connections
        for (unsigned i = 0; i < _conn_per_core; i++) {
            engine().net().connect(make_ipv4_address(server_addr)).then([this] (connected_socket fd) {
                _sockets.push_back(std::move(fd));
                // one connection done
                _conn_connected.signal();
            }).or_terminate();
        }

        // wait all connection done
        return _conn_connected.wait(_conn_per_core);
    }

    // run
    future<> run() {
        print("Established all %6d tcp connections on cpu %3d\n", _conn_per_core, engine().cpu_id());

        // 
        if (_timer_based) {
            _run_timer.arm(std::chrono::seconds(_duration));
        }

        // start ...
        for (auto&& fd : _sockets) {
            auto conn = make_lw_shared<connection>(std::move(fd), *this);

            // read
            repeat([conn, this] () {
                // recv all resp , then quit
                if (conn->_total_count == (uint64_t)_reqs_per_conn)
                {
                    // RTT time 
                    // RTT time 
                    // RTT time 
                    cout << "core: " << engine().cpu_id() << ", conn: ===> avg: " << (double)conn->_total_cost_time / (conn->_total_count) << ", max: " << conn->_max_time << ", min: " << conn->_min_time  << endl;
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }

                uint64_t sss = getCurrentTimeNS(); 

                // read data
                return conn->_in.read_exactly(buf_size).then([this, conn, sss] (auto&& data) {
                    uint64_t t = getCurrentTimeNS();
                    conn->_total_count++;
                    uint64_t tstart = *((uint64_t*)data.get());

                    // RTT time
                    // now time - send time (fill in the next repeat(write))
                    uint64_t duration = t - tstart;
                    conn->_max_time = std::max(conn->_max_time, duration);
                    conn->_min_time = std::min(conn->_min_time, duration);
                    conn->_total_cost_time += duration;

                    return make_ready_future<stop_iteration>(stop_iteration::no);
                });
            }).then([this, conn] {
                conn->_in.close().then([this]() {
                    ++_total_reqs;
                    // 
                    _conn_finished.signal();}
                );
            });

            // write
            repeat([conn, this] () {
                // quit when send all req
                if (conn->_total_sent == (uint64_t)_reqs_per_conn)
                {
                    return make_ready_future<stop_iteration>(stop_iteration::yes);
                }

                if (conn->index % buf_count == 0) {
                    // new a tmp buf for send data
                    //uint64_t s = getCurrentTimeNS();
		            conn->buffff = temporary_buffer<char>(total_size);
                    conn->index = 0;
                    //cout << "---------------> create new tmp buffer: " << getCurrentTimeNS()-s << endl;
                }

                // 
                uint64_t t = getCurrentTimeNS();
                // fill send time to the packet
                // when client recv the resp, use now_time - send time, that is the RTT time
                char * out_buf = conn->buffff.get_write() + conn->index * buf_size;
                *((uint64_t *)out_buf) = t;
                ++conn->index;
                ++conn->_total_sent;

                // zero copy pack
                net::fragment frag { out_buf, buf_size };
                net::packet pack(std::move(frag), deleter());

                // write data
                return conn->_out.write(std::move(pack)).then([this, conn] {
                    // flush or not
                    return make_ready_future<stop_iteration>(stop_iteration::no);
                    //return conn->_out.flush().then([]{
                    //    return make_ready_future<stop_iteration>(stop_iteration::no);
                    //});
                });
            }).then([conn] {
                conn->_out.close();
            });
            
        }

        // 
        return _conn_finished.wait(_conn_per_core);
    }

    future<> stop() {
        return make_ready_future();
    }
};

namespace bpo = boost::program_options;

int main(int ac, char** av) {
    // 
    distributed<tcp_echo_client> shard_echo_client;
    
    app_template app;
    app.add_options()
        ("server,s", bpo::value<std::string>()->default_value("127.0.0.1:10000"), "Server address")
        ("conn,c", bpo::value<unsigned>()->default_value(100), "total connections")
        ("reqs,r", bpo::value<unsigned>()->default_value(1), "reqs per connection")
        ("duration,d", bpo::value<unsigned>()->default_value(10), "duration of the test in seconds)");

    // run app
    return app.run(ac, av, [&] () -> future<int> {
        auto& config = app.configuration();
        auto server = config["server"].as<std::string>();
        auto reqs_per_conn = config["reqs"].as<unsigned>();
        auto total_conn= config["conn"].as<unsigned>();
        auto duration = config["duration"].as<unsigned>();

        // 每个core上的连接数量均分
        if (total_conn % smp::count != 0) {
            print("Error: conn needs to be n * cpu_nr\n");
            return make_ready_future<int>(-1);
        }

        auto started = steady_clock_type::now();

        print("========== tcp_echo_client ============\n");
        print("Server: %s\n", server);
        print("Connections: %u\n", total_conn);
        print("Requests/connection: %s\n", reqs_per_conn == 0 ? "dynamic (timer based)" : std::to_string(reqs_per_conn));

        return shard_echo_client.start(std::move(duration), std::move(total_conn), std::move(reqs_per_conn)).then([&shard_echo_client, server] {

            return shard_echo_client.invoke_on_all(&tcp_echo_client::connect, ipv4_addr{server});
        }).then([&shard_echo_client] {
            // 
            return shard_echo_client.invoke_on_all(&tcp_echo_client::run);
        }).then([&shard_echo_client] {
            // 
            return shard_echo_client.map_reduce(adder<uint64_t>(), &tcp_echo_client::total_reqs);
        }).then([&shard_echo_client, started] (uint64_t total_reqs) {
           auto finished = steady_clock_type::now();
           auto elapsed = finished - started;
           auto secs = static_cast<double>(elapsed.count() / 1000000000.0);
           print("Total cpus: %u\n", smp::count);
           print("Total requests: %u\n", total_reqs);
           print("Total time: %f\n", secs);
           print("Requests/sec: %f\n", static_cast<double>(total_reqs) / secs);
           print("==========     done     ============\n");

           return shard_echo_client.stop().then([] {
               return make_ready_future<int>(0);
           });
        });
    });
}


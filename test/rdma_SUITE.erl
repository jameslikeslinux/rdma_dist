%%%
%%% rdma_SUITE.erl
%%% Copyright (C) 2013 James Lee
%%%
%%% The contents of this file are subject to the Erlang Public License,
%%% Version 1.1, (the "License"); you may not use this file except in
%%% compliance with the License. You should have received a copy of the
%%% Erlang Public License along with this software. If not, it can be
%%% retrieved online at http://www.erlang.org/.
%%%
%%% Software distributed under the License is distributed on an "AS IS"
%%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%%% the License for the specific language governing rights and limitations
%%% under the License.
%%%

-module(rdma_SUITE).
-author("James Lee <jlee@thestaticvoid.com>").
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [
    test_listen_on_random_port,
    test_listen_on_specified_port,
    test_connect_to_valid_listener,
    test_connect_to_invalid_host,
    test_connect_to_invalid_port,
    test_accept_ignore_failed_connection,
    test_close_client_socket,
    test_close_server_socket,
    test_close_with_buffered_data,
    test_send_before_recv,
    test_recv_before_send,
    test_send_recv_binary,
    test_send_recv_zero_byte_buffer,
    test_send_more_than_num_buffers,
    test_send_larger_than_buffer_size,
    test_send_huge,
    test_active_socket
].

suite() -> [{timetrap, 5000}].

init_per_testcase(test_listen_on_random_port, Config) ->
    Config;

init_per_testcase(test_close_with_buffered_data, Config) ->
    create_listener(Config, [{num_buffers, 3}]);

init_per_testcase(test_send_recv_binary, Config) ->
    create_listener(Config, [binary]);

init_per_testcase(test_send_more_than_num_buffers, Config) ->
    create_listener(Config, [{num_buffers, 3}]);

init_per_testcase(test_send_larger_than_buffer_size, Config) ->
    create_listener(Config, [{buffer_size, 3}]);

init_per_testcase(test_send_huge, Config) ->
    create_listener(Config, [{num_buffers, 3}, {buffer_size, 3}]);

init_per_testcase(test_active_socket, Config) ->
    create_listener(Config, [{active, true}]);

init_per_testcase(_TestCase, Config) ->
    create_listener(Config, []).

end_per_testcase(test_listen_on_random_port, _Config) ->
    ok;

end_per_testcase(_TestCase, Config) ->
    rdma:close(?config(listener, Config)),
    ok.

%%
%% Test Cases
%%
test_listen_on_random_port(_Config) ->
    {ok, Listener} = rdma:listen(0),
    {ok, {_Address, PortNumber}} = rdma:sockname(Listener),
    true = PortNumber > 0,
    rdma:close(Listener),
    ok.

test_listen_on_specified_port(_Config) ->
    PortNumber = 12345,
    {ok, Listener} = rdma:listen(PortNumber),
    {ok, {_Address, PortNumber}} = rdma:sockname(Listener),
    rdma:close(Listener),
    ok.

test_connect_to_valid_listener(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config)),
    {ok, Server} = accept(Listener),
    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok.

test_connect_to_invalid_host(Config) ->
    {error, getaddrinfo} = rdma:connect("invalid-host", ?config(port_number, Config)),
    ok.

test_connect_to_invalid_port(_Config) ->
    {error, timeout} = rdma:connect("localhost", 1000),
    ok.

test_accept_ignore_failed_connection(Config) ->
    Listener = ?config(listener, Config),
    {error, timeout} = rdma:connect("localhost", ?config(port_number, Config)),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config)),
    {ok, Server} = accept(Listener),
    {error, timeout} = accept(Listener),
    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok.

test_close_client_socket(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config)),
    {ok, Server} = accept(Listener),
    Key = rpc:async_call(node(), rdma, recv, [Server]),
    ok = rdma:close(Client),
    {error, closed} = rpc:yield(Key),
    ok = rdma:close(Server),    % just to be sure
    ok.

test_close_server_socket(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config)),
    {ok, Server} = accept(Listener),
    Key = rpc:async_call(node(), rdma, recv, [Client]),
    ok = rdma:close(Server),
    {error, closed} = rpc:yield(Key),
    ok = rdma:close(Client),    % just to be sure
    ok.

test_close_with_buffered_data(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config), [{num_buffers, 3}]),
    {ok, Server} = accept(Listener),
    ok = rdma:send(Client, "test"),
    ok = rdma:send(Client, "test"),
    ok = rdma:send(Client, "test"),
    {ok, _, _, 1} = rdma:getstat(Client),   % one packet is buffered
    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok.

test_send_before_recv(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config)),
    {ok, Server} = accept(Listener),
    ok = rdma:send(Client, "test"),
    {ok, "test"} = rdma:recv(Server),
    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok.
    
test_recv_before_send(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config)),
    {ok, Server} = accept(Listener),
    Key = rpc:async_call(node(), rdma, recv, [Server]),
    timer:sleep(100),   % just to be sure recv is waiting
    ok = rdma:send(Client, "test"),
    {ok, "test"} = rpc:yield(Key),
    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok.

test_send_recv_binary(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config), [binary]),
    {ok, Server} = accept(Listener),
    ok = rdma:send(Client, <<"test">>),
    {ok, <<"test">>} = rdma:recv(Server),
    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok.

test_send_recv_zero_byte_buffer(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config)),
    {ok, Server} = accept(Listener),
    ok = rdma:send(Client, []),
    {ok, []} = rdma:recv(Server),
    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok.

test_send_more_than_num_buffers(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config), [{num_buffers, 3}]),
    {ok, Server} = accept(Listener),
    ok = rdma:send(Client, "test1"),
    ok = rdma:send(Client, "test2"),
    ok = rdma:send(Client, "test3"),
    ok = rdma:send(Client, "test4"),
    {ok, _, _, 2} = rdma:getstat(Client),      % two packets are buffered
    {ok, "test1"} = rdma:recv(Server),
    {ok, "test2"} = rdma:recv(Server),
    {error, timeout} = rdma:recv(Server, 100), % no packet available
    {ok, _, _, 2} = rdma:getstat(Client),      % two packets are still buffered
    {error, timeout} = rdma:recv(Client, 0),   % gets ack from server and flushes buffer
    {ok, _, _, 0} = rdma:getstat(Client),      % see?
    {ok, "test3"} = rdma:recv(Server),
    {ok, "test4"} = rdma:recv(Server),
    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok.

test_send_larger_than_buffer_size(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config), [{buffer_size, 3}]),
    {ok, Server} = accept(Listener),
    ok = rdma:send(Client, "foobar"),
    {ok, "foobar"} = rdma:recv(Server),
    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok.

test_send_huge(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config), [{num_buffers, 3}, {buffer_size, 3}]),
    {ok, Server} = accept(Listener),
    ok = rdma:send(Client, "foobarbaz"),
    {ok, _, _, 1} = rdma:getstat(Client),      % "baz" is buffered
    {error, timeout} = rdma:recv(Server, 100), % no full packet available
    {error, timeout} = rdma:recv(Client, 0),   % gets ack from server and flushes buffer
    {ok, _, _, 0} = rdma:getstat(Client),      % buffer flushed
    {ok, "foobarbaz"} = rdma:recv(Server),
    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok.

test_active_socket(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config), [{active, true}]),
    {ok, Server} = accept(Listener),
    ok = rdma:send(Client, "test"),

    % Current process isn't "owner" of Server.
    {error, timeout} = receive
        {Server, {data, "test"}} ->
            ok
    after 100 ->
        {error, timeout}
    end,
    ok = rdma:controlling_process(Server, self()),
    ok = rdma:send(Client, "test"),
    ok = receive
        {Server, {data, "test"}} ->
            ok
    after 100 ->
        {error, timeout}
    end,
    ok = rdma:close(Client),
    ok = receive
        {tcp_closed, Server} ->
            ok
    after 100 ->
        {error, timeout}
    end,
    ok = rdma:close(Server),
    ok.
    

%%
%% Helper Functions
%%
accept(Listener) ->
    receive
        {Listener, {accept, Socket}} ->
            {ok, Socket}
    after 1000 ->
        {error, timeout}
    end.

accept_loop(Listener, Pid) ->
    case rdma:accept(Listener) of
        {ok, Socket} ->
            Pid ! {Listener, {accept, Socket}},
            accept_loop(Listener, Pid);
        _Other ->
            ok
    end.

start_accept(Listener) ->
    spawn_link(?MODULE, accept_loop, [Listener, self()]).

create_listener(Config, Options) ->
    {ok, Listener} = rdma:listen(0, Options),
    {ok, {_Address, PortNumber}} = rdma:sockname(Listener),
    [{listener, Listener}, {port_number, PortNumber} | Config].

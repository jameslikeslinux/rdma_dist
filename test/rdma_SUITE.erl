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
    test_close_listener
%    test_send_before_recv,
%    test_recv_before_send,
%    test_send_recv_list,
%    test_send_recv_binary,
%    test_send_recv_zero_byte_buffer,
%    test_send_more_than_num_buffers,
%    test_send_larger_than_buffer_size,
%    test_active_socket
].

suite() -> [{timetrap, 5000}].

init_per_testcase(test_listen_on_random_port, Config) ->
    Config;

init_per_testcase(_TestCase, Config) ->
    {ok, Listener} = rdma:listen(0),
    {ok, {_Address, PortNumber}} = rdma:sockname(Listener),
    [{listener, Listener}, {port_number, PortNumber} | Config].

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
    Key2 = rpc:async_call(node(), rdma, recv, [Server]),
    ok = rdma:close(Client),
    {error, closed} = rpc:yield(Key2),
    ok = rdma:close(Server),    % just to be sure
    ok.

test_close_server_socket(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client} = rdma:connect("localhost", ?config(port_number, Config)),
    {ok, Server} = accept(Listener),
    Key2 = rpc:async_call(node(), rdma, recv, [Client]),
    ok = rdma:close(Server),
    {error, closed} = rpc:yield(Key2),
    ok = rdma:close(Client),    % just to be sure
    ok.

test_close_listener(Config) ->
    Listener = ?config(listener, Config),
    start_accept(Listener),
    {ok, Client1} = rdma:connect("localhost", ?config(port_number, Config)),
    {ok, Server1} = accept(Listener),
%    {ok, Client2} = rdma:connect("localhost", ?config(port_number, Config)),
%    {ok, Server2} = accept(Listener),
    RecvClient1Key = rpc:async_call(node(), rdma, recv, [Client1]),
%    RecvClient2Key = rpc:async_call(node(), rdma, recv, [Client2]),
%    RecvServer1Key = rpc:async_call(node(), rdma, recv, [Server1]),
%    RecvServer2Key = rpc:async_call(node(), rdma, recv, [Server2]),
    ok = rdma:close(Listener),
    {error, closed} = rpc:yield(RecvClient1Key),
%    {error, closed} = rpc:yield(RecvClient2Key),
%    {error, closed} = rpc:yield(RecvServer1Key),
%    {error, closed} = rpc:yield(RecvServer2Key),
    ok = rdma:close(Client1),    % just to be sure
%    ok = rdma:close(Client2),    % just to be sure
    ok = rdma:close(Server1),    % just to be sure
%    ok = rdma:close(Server2),    % just to be sure
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

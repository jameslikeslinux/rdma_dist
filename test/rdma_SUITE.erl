%%%
%%% rdma_SUITE.erl
%%% Copyright (C) 2013 James Lee
%%%
%%% This program is free software: you can redistribute it and/or modify
%%% it under the terms of the GNU General Public License as published by
%%% the Free Software Foundation, either version 3 of the License, or
%%% (at your option) any later version.
%%%
%%% This program is distributed in the hope that it will be useful,
%%% but WITHOUT ANY WARRANTY; without even the implied warranty of
%%% MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
%%% GNU General Public License for more details.
%%%
%%% You should have received a copy of the GNU General Public License
%%% along with this program. If not, see <http://www.gnu.org/licenses/>.
%%%

-module(rdma_SUITE).
-author("James Lee <jlee@thestaticvoid.com>").
-compile(export_all).

-include_lib("common_test/include/ct.hrl").

all() -> [
    test_rdma_listen,
    test_rdma_connect,
    test_rdma_accept,
    test_rdma_sockname,
    test_rdma_send_recv_binary,
    test_rdma_send_recv_list,
    test_rdma_close,
    test_rdma_controlling_process
].


%%
%% Test Cases
%%
test_rdma_listen(_Config) ->
    {ok, Listener1} = rdma:listen(0),
    ok = rdma:close(Listener1),
    ct:pal("Listens on a random port."),

    {ok, Listener2} = rdma:listen(12345),
    {ok, {_IPAddress, 12345}} = rdma:sockname(Listener2),
    ok = rdma:close(Listener2),
    ct:pal("Listens on a specified port."),

    {error, eacces} = rdma:listen(1000),
    ct:pal("Can't listen on a privileged port without privileges.").

acceptor(Listener) ->
    case rdma:accept(Listener) of
        {ok, _Socket} ->
            acceptor(Listener);
        _ ->
            ok
    end.

test_rdma_connect(_Config) ->
    {ok, Listener} = rdma:listen(12345),
    spawn(?MODULE, acceptor, [Listener]),
    {ok, Client1} = rdma:connect("localhost", 12345),
    ok = rdma:close(Client1),
    ct:pal("Connects to valid listener by name."),

    {ok, Client2} = rdma:connect({127,0,0,1}, 12345),
    ok = rdma:close(Client2),
    ct:pal("Connects to valid listener by IP."),

    {error, _} = rdma:connect("invalid-host", 12345),
    ct:pal("Doesn't connect to invalid host."),

    {error, _} = rdma:connect("localhost", 54321),
    ct:pal("Doesn't connect to invalid port."),

    ok = rdma:close(Listener),

    {error, _} = rdma:connect("localhost", 12345),
    ct:pal("Doesn't connect to a closed listener.").

test_rdma_accept(_Config) ->
    {ok, Listener} = rdma:listen(12345),
    spawn(?MODULE, acceptor, [Listener]),
    {ok, Client} = rdma:connect("localhost", 12345),
    ok = rdma:close(Client),
    ok = rdma:close(Listener).

test_rdma_sockname(_Config) ->
    PortNumber = 12345,
    {ok, Listener} = rdma:listen(PortNumber),
    spawn(?MODULE, acceptor, [Listener]),
    {ok, {{0,0,0,0}, PortNumber}} = rdma:sockname(Listener),
    ct:pal("Retrieves sockname from listening socket."),

    {ok, Client} = rdma:connect("localhost", PortNumber),
    {ok, _} = rdma:sockname(Client),
    ct:pal("Retrieves sockname from client socket."),

%    {ok, _} = rdma:sockname(Server),
%    ct:pal("Retrieves sockname from server socket."),

    ok = rdma:close(Client),
    ok = rdma:close(Listener),
    {error, closed} = rdma:sockname(Listener),
    ct:pal("Doesn't receive sockname from closed socket.").

test_rdma_send_recv_binary(_Config) ->
    {ok, Listener} = rdma:listen(12345, [binary]),
    {ok, Client} = rdma:connect("localhost", 12345, [binary]),
    {ok, Server} = rdma:accept(Listener),

    ok = rdma:send(Server, <<"foo">>),
    {ok, <<"foo">>} = rdma:recv(Client),
    ct:pal("Can send and receive a binary."),

    ok = rdma:send(Server, "bar"),
    {ok, <<"bar">>} = rdma:recv(Client),
    ct:pal("Can send a list and receive as a binary."),

    ok = rdma:send(Server, <<>>),
    {ok, <<>>} = rdma:recv(Client),
    ct:pal("Can send and receive a zero byte binary."),

    ok = rdma:close(Client),
    {error, closed} = rdma:send(Client, <<"baz">>),
    ct:pal("Can't send to a closed socket."),

    timer:sleep(100),
    {error, closed} = rdma:send(Server, <<"what">>),
    ct:pal("Server socket closes itself when client does."),

    ok = rdma:close(Server),
    ok = rdma:close(Listener).

test_rdma_send_recv_list(_Config) ->
    {ok, Listener} = rdma:listen(12345, [list]),
    {ok, Client} = rdma:connect("localhost", 12345, [list]),
    {ok, Server} = rdma:accept(Listener),

    ok = rdma:send(Server, "bar"),
    {ok, "bar"} = rdma:recv(Client),
    ct:pal("Can send and receive a list."),

    ok = rdma:send(Server, <<"foo">>),
    {ok, "foo"} = rdma:recv(Client),
    ct:pal("Can send a binary and receive as a list."),

    ok = rdma:send(Server, []),
    {ok, []} = rdma:recv(Client),
    ct:pal("Can send and receive a zero byte list."),

    ok = rdma:close(Server),
    timer:sleep(100),
    {error, closed} = rdma:send(Client, <<"what">>),
    ct:pal("Client socket closes itself when server does."),

    ok = rdma:close(Client),
    ok = rdma:close(Listener).

test_rdma_close(_Config) ->
    {ok, Listener1} = rdma:listen(12345),
    ok = rdma:close(Listener1),
    ct:pal("Closes socket normally."),

    ok = rdma:close(Listener1),
    ct:pal("Closes already closed socket."),

    {ok, Listener2} = rdma:listen(12345),
    TestCasePid = self(),
    spawn(fun() -> ok = rdma:close(Listener2), TestCasePid ! continue end),
    receive continue -> ok end,
    ct:pal("Closes socket from another process."),
    ok = rdma:close(Listener2).

test_rdma_controlling_process(_Config) ->
    {ok, Listener} = rdma:listen(12345),
    {ok, Client} = rdma:connect("localhost", 12345),
    {ok, Server} = rdma:accept(Listener),

    rdma:send(Client, "foo"),
    timer:sleep(100),

    TestCasePid = self(),
    Pid = spawn(fun() ->
        {ok, "foo"} = rdma:recv(Server, 100),
        {ok, "bar"} = rdma:recv(Server, 100),
        ok = rdma:close(Server),
        TestCasePid ! continue
    end),

    ok = rdma:controlling_process(Server, Pid),
    rdma:send(Client, "bar"),
    receive continue -> ok end,

    ct:pal("Can transfer controlling process of socket."),

    rdma:close(Client),
    rdma:close(Listener).

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
    test_rdma_port,
    test_rdma_send_recv
].


%%
%% Test Cases
%%
test_rdma_listen(_Config) ->
    {ok, Listener1} = rdma:listen(0),
    ok = rdma:close(Listener1),
    ct:pal("Listens on a random port."),

    {ok, Listener2} = rdma:listen(12345),
    {ok, 12345} = rdma:port(Listener2),
    ok = rdma:close(Listener2),
    ct:pal("Listens on a specified port."),

    {error, eacces} = rdma:listen(1000),
    ct:pal("Can't listen on a privileged port without privileges.").

test_rdma_connect(_Config) ->
    {ok, Listener} = rdma:listen(12345),
    {ok, Port} = rdma:port(Listener),
    {ok, Client} = rdma:connect("localhost", Port),
    ok = rdma:close(Client),
    ok = rdma:close(Listener).

test_rdma_accept(_Config) ->
    {ok, Listener} = rdma:listen(0),
    {ok, Port} = rdma:port(Listener),
    {ok, Client} = rdma:connect("localhost", Port),
    {ok, Server} = rdma:accept(Listener),
    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok = rdma:close(Listener).

test_rdma_port(_Config) ->
    Port = 12345,
    {ok, Listener} = rdma:listen(Port),
    {ok, Port} = rdma:port(Listener),
    ct:pal("Retrieves port number from listening socket."),

    rdma:close(Listener),
    {error, closed} = rdma:port(Listener),
    ct:pal("Doesn't receive port number from closed socket.").

test_rdma_send_recv(_Config) ->
    {ok, Listener} = rdma:listen(0),
    {ok, Port} = rdma:port(Listener),
    {ok, Client} = rdma:connect("localhost", Port),
    {ok, Server} = rdma:accept(Listener),

    {_, _, Micros1} = os:timestamp(),
    ok = rdma:send(Client, "test1"),
    {_, _, Micros2} = os:timestamp(),
    ok = rdma:send(Client, "test2"),
    {_, _, Micros3} = os:timestamp(),
    {ok, <<"test1">>} = rdma:recv(Server),
    {_, _, Micros4} = os:timestamp(),
    {ok, <<"test2">>} = rdma:recv(Server),
    {_, _, Micros5} = os:timestamp(),

    {ok, Time1} = rdma:time(Client),
    {ok, Time2} = rdma:time(Server),
    ct:pal("Took ~p micros to send 1.  Driver took ~p micros.", [Micros2 - Micros1, Time1]),
    ct:pal("Took ~p micros to send 2.  Driver took ~p micros.", [Micros3 - Micros2, Time1]),
    ct:pal("Took ~p micros to recv 1.  Driver took ~p micros.", [Micros4 - Micros3, Time2]),
    ct:pal("Took ~p micros to recv 2.  Driver took ~p micros.", [Micros5 - Micros4, Time2]),
    ct:pal("Complete time: ~p micros", [Micros5 - Micros1]),

    ok = rdma:send(Server, <<"test3">>),
    {ok, <<"test3">>} = rdma:recv(Client),

    ok = rdma:send(Server, <<"foobarbaz">>),
    {ok, <<"foobarbaz">>} = rdma:recv(Client),

    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok = rdma:close(Listener).

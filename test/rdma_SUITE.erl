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
    test_rdma_send_recv
].


%%
%% Test Cases
%%
test_rdma_listen(_Config) ->
    {ok, Socket} = rdma:listen(0),
    ok = rdma:close(Socket).

test_rdma_connect(_Config) ->
    {ok, Listener} = rdma:listen(0),
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

test_rdma_send_recv(_Config) ->
    {ok, Listener} = rdma:listen(0),
    {ok, Port} = rdma:port(Listener),
    {ok, Client} = rdma:connect("localhost", Port),
    {ok, Server} = rdma:accept(Listener),

    {_, _, Micros1} = os:timestamp(),
    ok = rdma:send(Client, "test"),
    {_, _, Micros2} = os:timestamp(),
    _Recv1 = rdma:recv(Server),
    {_, _, Micros3} = os:timestamp(),

    {ok, Time} = rdma:time(Client),
    ct:pal("Took ~p micros to send.  Driver took ~p micros.", [Micros2 - Micros1, Time]),
    ct:pal("Took ~p micros to recv", [Micros3 - Micros2]),
    ct:pal("Complete time: ~p micros", [Micros3 - Micros1]),

    ok = rdma:send(Server, "test"),
    _Recv2 = rdma:recv(Client),

    ok = rdma:close(Client),
    ok = rdma:close(Server),
    ok = rdma:close(Listener).

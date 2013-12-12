%%%
%%% rdma.erl
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

-module(rdma).
-author("James Lee <jlee@thestaticvoid.com>").

-export([connect/2, connect/3, connect/4, listen/1, listen/2, accept/1, port/1, send/2, recv/1, close/1, time/1]).

-define(DRV_CONNECT, $C).
-define(DRV_LISTEN, $L).
-define(DRV_PORT, $P).
-define(DRV_TIME, $T).


%%
%% API
%%
connect(Host, PortNumber) ->
    connect(Host, PortNumber, []).

connect(Host, PortNumber, Options) ->
    connect(Host, PortNumber, Options, 500).

connect(Host, PortNumber, Options, Timeout)->
    load_driver(),
    Port = open_port({spawn, "rdma_drv"}, []),

    HostStr = case inet:ntoa(Host) of
        {error, einval} -> Host;
        Address         -> Address
    end,

    case control(Port, ?DRV_CONNECT, term_to_binary([{dest_host, HostStr}, {dest_port, integer_to_list(PortNumber)}, {timeout, Timeout} | prepare_options_list(Options)])) of
        ok ->
            receive
                {Port, established} ->
                    {ok, Port};

                {Port, error, Reason} ->
                    close(Port),
                    {error, Reason}
            end;

        Error ->
            close(Port),
            Error
    end.

listen(PortNumber) ->
    listen(PortNumber, []).

listen(PortNumber, Options) ->
    load_driver(),
    Port = open_port({spawn, "rdma_drv"}, []),
    case control(Port, ?DRV_LISTEN, term_to_binary([{port, PortNumber} | prepare_options_list(Options)])) of
        ok -> 
            {ok, Port};

        Error ->
            close(Port),
            Error
    end.

accept(Socket) ->
    Self = self(),
    case erlang:port_info(Socket, connected) of
        {connected, Self} ->
            receive
                {Socket, accept, ClientPort} -> {ok, ClientPort}
            end;

        undefined -> {error, closed};
        _Other    -> {error, not_owner}
    end.

port(Socket) ->
    case catch control(Socket, ?DRV_PORT, []) of
        {'EXIT', {badarg, _}} -> {error, closed};
        {ok, 0}               -> {error, not_bound};
        Other                 -> Other
    end.

send(Socket, Data) ->
    case erlang:port_info(Socket, connected) of
        undefined ->
            {error, closed};

        _Else ->
            port_command(Socket, Data),
            ok
    end.

recv(Socket) ->
    Self = self(),
    case erlang:port_info(Socket, connected) of
        {connected, Self} ->
            receive
                {Socket, data, Data} -> {ok, Data}
            end;

        undefined -> {error, closed};
        _Other    -> {error, not_owner}
    end.

close(Socket) ->
    port_close(Socket),
    ok.

time(Socket) ->
    case catch control(Socket, ?DRV_TIME, []) of
        {'EXIT', {badarg, _}} -> {error, closed};
        Other                 -> Other
    end.


%%
%% Private Functions
%%
load_driver() ->
    case erl_ddll:load_driver(code:priv_dir(rdma_dist), "rdma_drv") of
        ok                      -> ok;
        {error, already_loaded} -> ok;
        E                       -> exit(E)
    end.

control(Port, Command, Args) ->
    binary_to_term(port_control(Port, Command, Args)).

prepare_options_list(Options) ->
    % replace tuple ip address representation with string, if any
    case proplists:get_value(ip, Options) of
        undefined -> Options;
        IpAddress -> [{ip, inet:ntoa(IpAddress)} | proplists:delete(ip, Options)]
    end.

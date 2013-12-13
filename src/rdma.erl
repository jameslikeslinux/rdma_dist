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

-export([connect/2, connect/3, connect/4, listen/1, listen/2, accept/1, accept/2, port_number/1, send/2, recv/1, recv/2, close/1, controlling_process/2]).

-define(DRV_CONNECT, $C).
-define(DRV_LISTEN, $L).
-define(DRV_PORT_NUMBER, $P).
-define(DRV_DISCONNECT, $D).
-define(DRV_PAUSE, $p).
-define(DRV_RESUME, $r).


%%
%% API
%%
connect(Host, PortNumber) ->
    connect(Host, PortNumber, []).

connect(Host, PortNumber, Options) ->
    connect(Host, PortNumber, Options, 500).

connect(Host, PortNumber, Options, Timeout)->
    load_driver(),
    Socket = open_port({spawn, "rdma_drv"}, []),

    HostStr = case inet:ntoa(Host) of
        {error, einval} ->
            Host;
        Address ->
            Address
    end,

    case control(Socket, ?DRV_CONNECT, term_to_binary([{dest_host, HostStr}, {dest_port, integer_to_list(PortNumber)}, {timeout, Timeout} | prepare_options_list(Options)])) of
        ok ->
            receive
                {Socket, established} ->
                    {ok, Socket};
                {Socket, error, Reason} ->
                    close(Socket),
                    {error, Reason}
            end;
        {error, Reason} ->
            close(Socket),
            {error, Reason}
    end.

listen(PortNumber) ->
    listen(PortNumber, []).

listen(PortNumber, Options) ->
    load_driver(),
    Socket = open_port({spawn, "rdma_drv"}, []),
    case control(Socket, ?DRV_LISTEN, term_to_binary([{port, PortNumber} | prepare_options_list(Options)])) of
        ok -> 
            {ok, Socket};
        {error, Reason} ->
            close(Socket),
            {error, Reason}
    end.

accept(Socket) ->
    accept(Socket, infinity).

accept(Socket, Timeout) ->
    Self = self(),
    case erlang:port_info(Socket, connected) of
        {connected, Self} ->
            receive
                {Socket, accept, ClientPort} ->
                    {ok, ClientPort};
                {Socket, error, Reason} ->
                    close(Socket),
                    {error, Reason}
            after Timeout ->
                {error, timeout}
            end;
        {connected, _} ->
            {error, not_owner};
        undefined ->
            {error, closed}
    end.

port_number(Socket) ->
    case catch control(Socket, ?DRV_PORT_NUMBER, []) of
        {ok, 0} ->
            {error, not_bound};
        {ok, PortNumber} ->
            {ok, PortNumber};
        {'EXIT', {badarg, _}} ->
            {error, closed}
    end.

send(Socket, Data) ->
    Self = self(),
    case erlang:port_info(Socket, connected) of
        {connected, Self} ->
            receive
                {Socket, error, Reason} ->
                    close(Socket),
                    {error, Reason}
            after 0 ->
                case catch port_command(Socket, Data) of
                    true ->
                        ok;
                    {'EXIT', {badarg, _}} ->
                        {error, closed}
                end
            end;
        {connected, _} ->
            {error, not_owner};
        undefined ->
            {error, closed}
    end.

recv(Socket) ->
    recv(Socket, infinity).

recv(Socket, Timeout) ->
    Self = self(),
    case erlang:port_info(Socket, connected) of
        {connected, Self} ->
            receive
                {Socket, data, Data} ->
                    {ok, Data};
                {Socket, error, Reason} ->
                    close(Socket),
                    {error, Reason}
            after Timeout ->
                {error, timeout}
            end;
        {connected, _} ->
            {error, not_owner};
        undefined ->
            {error, closed}
    end.

close(Socket) ->
    Self = self(),
    case erlang:port_info(Socket, connected) of
        {connected, Self} ->
            case control(Socket, ?DRV_DISCONNECT, []) of
                wait ->
                    receive
                        {Socket, disconnected} ->
                            close_port(Socket),
                            ok;
                        {Socket, error, Reason} ->
                            close_port(Socket),
                            {error, Reason}
                    end;
                ok ->
                    close_port(Socket),
                    ok;
                {error, Reason} ->
                    close_port(Socket),
                    {error, Reason}
            end;
        {connected, _} ->
            {error, not_owner};
        undefined ->
            ok
    end.

controlling_process(Socket, Pid) ->
    % XXX: Rework, checking caller, return values.
    catch control(Socket, ?DRV_PAUSE, []),
    case catch port_connect(Socket, Pid) of
        true ->
            catch unlink(Socket),
            forward(Socket, Pid),
            catch control(Socket, ?DRV_RESUME, []),
            ok;
        {'EXIT', {badarg, _}} ->
            {error, closed}
    end.


%%
%% Private Functions
%%
load_driver() ->
    case erl_ddll:load_driver(code:priv_dir(rdma_dist), "rdma_drv") of
        ok ->
            ok;
        {error, already_loaded} ->
            ok;
        Error ->
            exit(Error)
    end.

control(Port, Command, Args) ->
    binary_to_term(port_control(Port, Command, Args)).

prepare_options_list(Options) ->
    % Replace tuple ip address representation with string, if any.
    case proplists:get_value(ip, Options) of
        undefined ->
            Options;
        IpAddress ->
            [{ip, inet:ntoa(IpAddress)} | proplists:delete(ip, Options)]
    end.

close_port(Socket) ->
    case catch erlang:port_close(Socket) of
        true ->
            ok;
        {'EXIT', {badarg, _}} ->
            % Socket is already closed.
            ok
    end.

forward(Socket, Pid) ->
    receive
        {Socket, A} ->
            Pid ! {Socket, A},
            forward(Socket, Pid);
        {Socket, A, B} ->
            Pid ! {Socket, A, B},
            forward(Socket, Pid)
    after 0 ->
        ok
    end.

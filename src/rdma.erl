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

-export([connect/2, connect/3, connect/4, listen/1, listen/2, accept/1, accept/2, peername/1, sockname/1, send/2, recv/1, recv/2, close/1, controlling_process/2, tick/1, getstat/1, setopts/2]).

-define(DRV_CONNECT, $C).
-define(DRV_LISTEN, $L).
-define(DRV_ACCEPT, $A).
-define(DRV_PEERNAME, $P).
-define(DRV_SOCKNAME, $S).
-define(DRV_RECV, $R).
-define(DRV_DISCONNECT, $D).
-define(DRV_STATS, $s).
-define(DRV_SETOPTS, $O).

-define(check_server(), case whereis(rdma_server) of
    undefined ->
        exit(rdma_server_not_started);
    _ ->
        ok
end).


%%
%% API
%%
connect(Host, PortNumber) ->
    connect(Host, PortNumber, []).

connect(Host, PortNumber, Options) ->
    connect(Host, PortNumber, Options, 500).

connect(Host, PortNumber, Options, Timeout)->
    ?check_server(),
    Socket = open_port({spawn, "rdma_drv"}, [{packet, 4}]),

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
            after Timeout ->
                close(Socket),
                {error, timeout}
            end;
        {error, Reason} ->
            close(Socket),
            {error, Reason}
    end.

listen(PortNumber) ->
    listen(PortNumber, []).

listen(PortNumber, Options) ->
    ?check_server(),
    Socket = open_port({spawn, "rdma_drv"}, [{packet, 4}]),
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
    case catch control(Socket, ?DRV_ACCEPT, []) of
        ok ->
            receive
                {Socket, {port, ClientPort}} ->
                    receive
                        {Socket, {accept, ClientPort}} ->
                            {ok, ClientPort};
                        {Socket, {error, _Reason}} ->
                            close(ClientPort),
                            accept(Socket, Timeout)
                    after Timeout ->
                        {error, timeout}
                    end;
                {Socket, {error, Reason}} ->
                    {error, Reason}
            after Timeout ->
                {error, timeout}
            end;
        {error, Reason} ->
            {error, Reason};
        {'EXIT', {badarg, _}} ->
            {error, closed}
    end.

peername(Socket) ->
    case catch control(Socket, ?DRV_PEERNAME, []) of
        {ok, {Address, PortNumber}} ->
            case inet:parse_address(Address) of
                {ok, IPAddress} ->
                    {ok, {IPAddress, PortNumber}};
                Error ->
                    Error
            end;
        {'EXIT', {badarg, _}} ->
            {error, closed}
    end.

sockname(Socket) ->
    case catch control(Socket, ?DRV_SOCKNAME, []) of
        {ok, {Address, PortNumber}} ->
            case inet:parse_address(Address) of
                {ok, IPAddress} ->
                    {ok, {IPAddress, PortNumber}};
                Error ->
                    Error
            end;
        {'EXIT', {badarg, _}} ->
            {error, closed}
    end.

send(Socket, Data) ->
    send(Socket, Data, []).

send(Socket, Data, Options) ->
    case catch port_command(Socket, Data, Options) of
        true ->
            receive
                {Socket, {error, Reason}} ->
                    close(Socket),
                    {error, Reason}
            after 0 ->
                ok
            end;
        {'EXIT', {badarg, _}} ->
            {error, closed}
    end.

recv(Socket) ->
    recv(Socket, infinity).

recv(Socket, Timeout) ->
    case catch control(Socket, ?DRV_RECV, []) of
        ok ->
            receive
                {Socket, {data, Data}} ->
                    {ok, Data};
                {Socket, {error, Reason}} ->
                    close(Socket),
                    {error, Reason} 
            after Timeout ->
                {error, timeout}
            end;
        {error, Reason} ->
            % Reasons like 'not_connected'...so don't close socket.
            {error, Reason};
        {'EXIT', {badarg, _}} ->
            {error, closed}
    end.

close(Socket) ->
    case catch control(Socket, ?DRV_DISCONNECT, []) of
        wait ->
            receive
                {Socket, disconnected} ->
                    close_port(Socket);
                {Socket, {error, Reason}} ->
                    close_port(Socket),
                    {error, Reason}
            end;
        ok ->
            close_port(Socket);
        {error, Reason} ->
            close_port(Socket),
            {error, Reason};
        {'EXIT', {badarg, _}} ->
            ok
    end.

controlling_process(Socket, Pid) ->
    case catch port_connect(Socket, Pid) of
        true ->
            catch unlink(Socket),
            ok;
        {'EXIT', {badarg, _}} ->
            {error, closed}
    end.

tick(Socket) ->
    send(Socket, [], [force]).

getstat(Socket) ->
    case catch control(Socket, ?DRV_STATS, []) of
        {ok, R, S, Q} ->
            {ok, R, S, Q};
        {'EXIT', {badarg, _}} ->
            {error, closed}
    end.

setopts(Socket, Options) ->
    case catch control(Socket, ?DRV_SETOPTS, term_to_binary(prepare_options_list(Options))) of
        ok ->
            ok;
        {error, Reason} ->
            {error, Reason};
        {'EXIT', {badarg, _}} ->
            {error, closed}
    end.

% XXX: Create a cancel operation.


%%
%% Private Functions
%%
control(Port, Command, Args) ->
    binary_to_term(port_control(Port, Command, Args)).

prepare_options_list(Options) ->
    % Turn atoms like 'active' into '{active, true}'.
    NewOptions = proplists:unfold(Options),

    % Replace tuple ip address representation with string, if any.
    case proplists:get_value(ip, NewOptions) of
        undefined ->
            NewOptions;
        IpAddress ->
            [{ip, inet:ntoa(IpAddress)} | proplists:delete(ip, NewOptions)]
    end.

close_port(Socket) ->
    case catch erlang:port_close(Socket) of
        true ->
            ok;
        {'EXIT', {badarg, _}} ->
            % Socket is already closed.
            ok
    end.

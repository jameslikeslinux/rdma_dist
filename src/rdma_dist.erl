%%%
%%% rdma_dist.erl
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

%%%
%%% Portions of this code are modified from inet6_tcp_dist.erl and
%%% uds_dist.erl in the Erlang/OTP distribution, which is Copyright
%%% (C) Ericsson AB 1997-2013 and used under the terms of the Erlang
%%% Public License, Version 1.1.
%%%

-module(rdma_dist).
-author("James Lee <jlee@thestaticvoid.com>").

-export([childspecs/0, listen/1, accept/1, accept_connection/5,
         setup/5, close/1, select/1, is_node_name/1]).

%% internal exports
-export([accept_loop/2, do_accept/6, do_setup/6]).

-import(error_logger,[error_msg/2]).

-include_lib("kernel/include/dist.hrl").
-include_lib("kernel/include/dist_util.hrl").
-include_lib("kernel/include/net_address.hrl").

childspecs() ->
    {ok, [{rdma_server,{rdma_server, start_link, []},
           permanent, 2000, worker, [rdma_server]}]}.

%% ------------------------------------------------------------
%% Select this protocol based on node name
%% select(Node) => Bool
%% ------------------------------------------------------------
select(Node) ->
    case split_node(atom_to_list(Node), $@, []) of
        [_, Host] ->
            case inet:getaddr(Host, inet) of
                {ok, _} ->
                    true;
                _ ->
                    false
            end;
        _ ->
            false
    end.


%% ------------------------------------------------------------
%% Create the listen socket, i.e. the port that this erlang
%% node is accessible through.
%% ------------------------------------------------------------
listen(Name) ->
    case rdma:listen(0, [{packet, 4}]) of
        {ok, Listener} ->
            {ok, Address = {_, Port}} = rdma:sockname(Listener),
            {ok, Host} = inet:gethostname(),
            {ok, Creation} = erl_epmd:register_node(Name, Port),
            {ok, {Listener, #net_address{
                address = Address,
                host = Host,
                protocol = rdma,
                family = rdma
            }, Creation}};
        Error ->
            Error
    end.

%% ------------------------------------------------------------
%% Accepts new connection attempts from other Erlang nodes.
%% ------------------------------------------------------------
accept(Listener) ->
    spawn_opt(?MODULE, accept_loop, [self(), Listener], [link, {priority, max}]).

accept_loop(Kernel, Listener) ->
    case rdma:accept(Listener) of
        {ok, Socket} ->
            Kernel ! {accept,self(),Socket,rdma,rdma},
            controller(Kernel, Socket),
            accept_loop(Kernel, Listener);
        Error ->
            exit(Error)
    end.

controller(Kernel, Socket) ->
    receive
        {Kernel, controller, Pid} ->
            ok = rdma:controlling_process(Socket, Pid),
            Pid ! {self(), controller};
        {Kernel, unsupported_protocol} ->
            exit(unsupported_protocol)
    end.

%% ------------------------------------------------------------
%% Accepts a new connection attempt from another Erlang node.
%% Performs the handshake with the other side.
%% ------------------------------------------------------------
accept_connection(AcceptPid, Socket, MyNode, Allowed, SetupTime) ->
    spawn_link(?MODULE, do_accept, [self(), AcceptPid, Socket, MyNode, Allowed, SetupTime]).

do_accept(Kernel, AcceptPid, Socket, MyNode, Allowed, SetupTime) ->
    receive
        {AcceptPid, controller} ->
            Timer = dist_util:start_timer(SetupTime),
            HSData = #hs_data{
                kernel_pid = Kernel,
                this_node = MyNode,
                socket = Socket,
                timer = Timer,
                this_flags = 0,
                allowed = Allowed,
                f_send = fun(S,D) -> rdma:send(S,D) end,
                f_recv = fun(S,_N,T) -> rdma:recv(S,T) end,
                f_setopts_pre_nodeup = fun(S) -> rdma:setopts(S, [{active, false}]) end,
                f_setopts_post_nodeup = fun(S) -> rdma:setopts(S, [{active, true}]) end,
                f_getll = fun(S) -> {ok, S} end,
                f_address = fun get_remote_id/2,
                mf_tick = fun rdma:tick/1,
                mf_getstat = fun rdma:getstat/1
            },
            dist_util:handshake_other_started(HSData)
    end.

get_remote_id(Socket, Node) ->
    {ok, Address} = rdma:peername(Socket),
    [_, Host] = split_node(atom_to_list(Node), $@, []),
    #net_address{
        address = Address,
        host = Host,
        protocol = rdma,
        family = rdma
    }.

%% ------------------------------------------------------------
%% Setup a new connection to another Erlang node.
%% Performs the handshake with the other side.
%% ------------------------------------------------------------
setup(Node, Type, MyNode, LongOrShortNames,SetupTime) ->
    spawn_opt(?MODULE, do_setup,
        [self(), Node, Type, MyNode, LongOrShortNames, SetupTime],
        [link, {priority, max}]).

do_setup(Kernel, Node, Type, MyNode, LongOrShortNames, SetupTime) ->
    [Name, Address] = splitnode(Node, LongOrShortNames),
    case inet:getaddr(Address, inet) of
        {ok, Ip} ->
            Timer = dist_util:start_timer(SetupTime),
            case erl_epmd:port_please(Name, Ip) of
                {port, TcpPort, Version} ->
                    dist_util:reset_timer(Timer),
                    case rdma:connect(Ip, TcpPort, [{packet, 4}]) of
                        {ok, Socket} ->
                            HSData = #hs_data{
                                kernel_pid = Kernel,
                                other_node = Node,
                                this_node = MyNode,
                                socket = Socket,
                                timer = Timer,
                                this_flags = 0,
                                other_version = Version,
                                f_send = fun(S,D) -> rdma:send(S,D) end,
                                f_recv = fun(S,_N,T) -> rdma:recv(S,T) end,
                                f_setopts_pre_nodeup = fun(S) -> rdma:setopts(S, [{active, false}]) end,
                                f_setopts_post_nodeup = fun(S) -> rdma:setopts(S, [{active, true}]) end,
                                f_getll = fun(S) -> {ok, S} end,
                                f_address = fun(_, _) ->
                                    #net_address{
                                        address = {Ip, TcpPort},
                                        host = Address,
                                        protocol = rdma,
                                        family = rdma
                                    }
                                end,
                                mf_tick = fun rdma:tick/1,
                                mf_getstat = fun rdma:getstat/1,
                                request_type = Type
                            },
                            dist_util:handshake_we_started(HSData);
                        _ ->
                            error_msg("connect~n", []),
                            ?shutdown(Node)
                    end;
                _->
                    error_msg("port_please~n", []),
                    ?shutdown(Node)
            end;
        _ ->
            error_msg("getaddr~n", []),
            ?shutdown(Node)
    end.

%%
%% Close a socket.
%%
close(Socket) ->
    rdma:close(Socket).

%% If Node is illegal terminate the connection setup!!
splitnode(Node, LongOrShortNames) ->
    case split_node(atom_to_list(Node), $@, []) of
        [Name|Tail] when Tail =/= [] ->
            Host = lists:append(Tail),
            case split_node(Host, $., []) of
                [_] when LongOrShortNames =:= longnames ->
                    case inet:parse_ipv4strict_address(Host) of
                        {ok, _} ->
                            [Name, Host];
                        _ ->
                            error_msg("** System running to use "
                                      "fully qualified "
                                      "hostnames **~n"
                                      "** Hostname ~s is illegal **~n",
                                      [Host]),
                            ?shutdown(Node)
                    end;
                L when length(L) > 1, LongOrShortNames =:= shortnames ->
                    error_msg("** System NOT running to use fully qualified "
                              "hostnames **~n"
                              "** Hostname ~s is illegal **~n",
                              [Host]),
                    ?shutdown(Node);
                _ ->
                    [Name, Host]
            end;
        [_] ->
            error_msg("** Nodename ~p illegal, no '@' character **~n",
                      [Node]),
            ?shutdown(Node);
        _ ->
            error_msg("** Nodename ~p illegal **~n", [Node]),
            ?shutdown(Node)
    end.

split_node([Chr|T], Chr, Ack) -> [lists:reverse(Ack)|split_node(T, Chr, [])];
split_node([H|T], Chr, Ack) -> split_node(T, Chr, [H|Ack]);
split_node([], _, Ack) -> [lists:reverse(Ack)].

is_node_name(Node) when is_atom(Node) ->
    case split_node(atom_to_list(Node), $@, []) of
        [_,_Host] -> true;
        _ -> false
    end;
is_node_name(_Node) ->
    false.

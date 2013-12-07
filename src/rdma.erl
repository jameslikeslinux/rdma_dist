-module(rdma).
-author("James Lee <jlee@thestaticvoid.com>").
-behavior(gen_fsm).

-export([connect/2, listen/1, accept/1, port/1, send/2, recv/1, close/1]).
-export([init/1, disconnected/3, connecting/3, connected/3, closed/3, waiting/3, listening/3, handle_sync_event/4, handle_info/3]).

-record(state_data, {port, queue, caller}).

-define(DRV_CONNECT, $C).
-define(DRV_LISTEN, $L).
-define(DRV_PORT, $P).
-define(DRV_POLL, $p).


%%
%% API
%%
connect(Host, Port) ->
    {ok, Socket} = start_link(),
    case gen_fsm:sync_send_event(Socket, {connect, Host, Port}) of
        ok    -> {ok, Socket};
        Other -> Other
    end.

listen(Port) ->
    {ok, Socket} = start_link(),
    case gen_fsm:sync_send_event(Socket, {listen, Port}) of
        ok    -> {ok, Socket};
        Other -> Other
    end.

accept(Socket) ->
    gen_fsm:sync_send_event(Socket, accept, infinity).

port(Socket) ->
    gen_fsm:sync_send_event(Socket, port).

send(Socket, Data) ->
    gen_fsm:sync_send_event(Socket, {send, Data}, infinity).

recv(Socket) ->
    gen_fsm:sync_send_event(Socket, recv, infinity).

close(Socket) ->
    gen_fsm:sync_send_all_state_event(Socket, close).

%%
%% Callbacks
%%
init([]) ->
    Port = open_port({spawn, "rdma_drv"}, []),
    {ok, disconnected, #state_data{port = Port, queue = queue:new()}};

init(Port) ->
    port_connect(Port, self()),
    % only start polling the cq after the port's owner has been switched
    port_sync_control(Port, ?DRV_POLL, []),
    {ok, connected, #state_data{port = Port, queue = queue:new()}}.

disconnected({connect, Host, PortNumber}, From, StateData = #state_data{port = Port}) ->
    port_async_control(Port, ?DRV_CONNECT, [Host, $:, integer_to_list(PortNumber)]),
    {next_state, connecting, StateData#state_data{caller = From}};

disconnected({listen, _PortNumber}, _From, StateData = #state_data{port = Port}) ->
    case port_sync_control(Port, ?DRV_LISTEN, []) of
        ok    -> {reply, ok, listening, StateData};
        Other -> {reply, {error, Other}, disconnected, StateData}
    end.

connecting(established, _From, StateData) ->
    {reply, ok, connected, StateData}.

connected({send, Data}, From, StateData = #state_data{port = Port}) ->
    port_command(Port, Data),
    {next_state, waiting, StateData#state_data{caller = From}};

connected(recv, From, StateData = #state_data{queue = RecvQueue}) ->
    case queue:out(RecvQueue) of
        {{value, Data}, NewRecvQueue} ->
            {reply, {ok, Data}, connected, StateData#state_data{queue = NewRecvQueue}};

        {empty, RecvQueue} ->
            {next_state, waiting, StateData#state_data{caller = From}}
    end;

connected({recv, Data}, _From, StateData = #state_data{queue = RecvQueue}) ->
    % insert data into receive queue
    {next_state, connected, StateData#state_data{queue = queue:in(Data, RecvQueue)}}.

closed(_Event, _From, StateData) ->
    {reply, {error, closed}, closed, StateData}.

listening(accept, From, StateData = #state_data{queue = AcceptQueue}) ->
    case queue:out(AcceptQueue) of
        {{value, ClientSocket}, NewAcceptQueue} ->
            {reply, {ok, ClientSocket}, listening, StateData#state_data{queue = NewAcceptQueue}};

        {empty, AcceptQueue} ->
            {next_state, waiting, StateData#state_data{caller = From}}
    end;

listening({accept, Port}, _From, StateData = #state_data{queue = AcceptQueue}) ->
    {ok, Socket} = start_link(Port),
    {next_state, listening, StateData#state_data{queue = queue:in(Socket, AcceptQueue)}};

listening(port, _From, StateData = #state_data{port = Port}) ->
    Reply = port_sync_control(Port, ?DRV_PORT, []),
    {reply, Reply, listening, StateData}.

waiting(sent, _From, StateData) ->
    {reply, ok, connected, StateData};

waiting({recv, Data}, _From, StateData) ->
    {reply, {ok, Data}, connected, StateData};

waiting({accept, Port}, _From, StateData) ->
    {ok, Socket} = start_link(Port),
    {reply, {ok, Socket}, listening, StateData}.

handle_sync_event(close, _From, _State, StateData) ->
    port_close(StateData#state_data.port),
    {reply, ok, closed, StateData}.   

handle_info({event, Event}, State, StateData = #state_data{caller = Caller}) ->
    case ?MODULE:State(Event, Caller, StateData) of
        {reply, Reply, NextState, NewStateData} ->
            gen_fsm:reply(Caller, Reply),
            {next_state, NextState, NewStateData};

        Other ->
            Other
    end.

%%
%% Private Functions
%%
start_link() ->
    case erl_ddll:load_driver(code:priv_dir(rdma_dist), "rdma_drv") of
        ok                      -> ok;
        {error, already_loaded} -> ok;
        E                       -> exit(E)
    end,
    gen_fsm:start_link(?MODULE, [], []).

start_link(Port) ->
    gen_fsm:start_link(?MODULE, Port, []).

port_sync_control(Port, Command, Args) ->
    binary_to_term(port_control(Port, Command, Args)).

port_async_control(Port, Command, Args) ->
    port_control(Port, Command, Args).

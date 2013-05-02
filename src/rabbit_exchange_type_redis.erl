-module(rabbit_exchange_type_redis).
-include("redis_exchange.hrl").
-behaviour(rabbit_exchange_type).

-define(EXCHANGE_TYPE_BIN,  <<"x-redis">>).
-define(HOST,               <<"host">>).
-define(PORT,               <<"port">>).
-define(MAX_CLIENTS,        <<"maxclients">>).
-define(TYPE,               <<"type-module">>).

-rabbit_boot_step({?MODULE, [
  {description,   "exchange type redis"},
  {mfa,           {rabbit_registry, register, [exchange, ?EXCHANGE_TYPE_BIN, ?MODULE]}},
  {requires,      rabbit_registry},
  {enables,       kernel_ready}
]}).

-export([
  add_binding/3, 
  assert_args_equivalence/2,
  create/2, 
  delete/3, 
  policy_changed/2,
  description/0, 
  recover/2, 
  remove_bindings/3,
  route/2,
  serialise_events/0,
  validate/1,
  validate_binding/2
]).

description() ->
  [{name, ?EXCHANGE_TYPE_BIN}, {description, <<"exchange type redis">>}].

serialise_events() -> 
  false.

validate(X) ->
%    io:format("Validate passed: ~w~n", [X]),
    Exchange = exchange_type(X),
    Exchange:validate(X).

validate_binding(_X, _B) -> ok.
  
create(Tx, X = #exchange{name = #resource{virtual_host=_VirtualHost, name=_Name}, arguments = _Args}) ->
  XA = exchange_a(X),
  pg2:create(XA),
  
  case get_redis_client(X) of
    {ok, _Client} ->
      Exchange = exchange_type(X),
      Exchange:create(Tx, X);
    _ -> 
      error_logger:error_msg("Could not connect to Redis"),
      {error, "could not connect to redis"}
  end.

recover(X, _Bs) ->
  create(none, X).

delete(Tx, X, Bs) ->
  XA = exchange_a(X),
  pg2:delete(XA),
  Exchange = exchange_type(X),
  Exchange:delete(Tx, X, Bs).

policy_changed(_X1, _X2) -> ok.

add_binding(Tx, X, B) ->
  Exchange = exchange_type(X),
  Exchange:add_binding(Tx, X, B).

remove_bindings(Tx, X, Bs) ->
  Exchange = exchange_type(X),
  Exchange:remove_bindings(Tx, X, Bs).

assert_args_equivalence(X, Args) ->
  rabbit_exchange:assert_args_equivalence(X, Args).
  
route(X=#exchange{name = #resource{virtual_host = _VirtualHost, name = _Name}}, 
      D=#delivery{message = _Message0 = #basic_message{routing_keys = Routes, content = Content0}}) ->
  #content{
    properties = _Props = #'P_basic'{ 
      content_type = _CT, 
      headers = _Headers, 
      reply_to = _ReplyTo
    },
    payload_fragments_rev = PayloadRev
  } = rabbit_binary_parser:ensure_content_decoded(Content0),


%% Redis pub/sub - we publish the message to Redis using all Routes 

 case get_redis_client(X) of
    {ok, Client} ->
      % Convert payload to list, concat together
      Payload = lists:foldl(fun(Chunk, NewPayload) ->
        <<Chunk/binary, NewPayload/binary>>
      end, <<>>, PayloadRev),

      lists:foldl(fun(Route, _) ->
%%        io:format("Route: ~p~n", [Route]),
        _Result = eredis:q(Client, ["PUBLISH", Route, Payload])
%%        io:format("result: ~p~n", [Result])
      end, [], Routes);
    _Err ->
      %io:format("err: ~p~n", [Err]),
      error_logger:error_msg("Could not connect to Redis server")
  end,

  Exchange = exchange_type(X),
  Exchange:route(X, D).
  
exchange_a(#exchange{name = #resource{virtual_host=VirtualHost, name=Name}}) ->
    list_to_atom(lists:flatten(io_lib:format("~s ~s", [VirtualHost, Name]))).
  
get_redis_client(X=#exchange{arguments = Args}) ->
  Host = case lists:keyfind(?HOST, 1, Args) of
     {_, _, H} -> binary_to_list(H);
             _ -> "127.0.0.1"
  end,
  Port = case lists:keyfind(?PORT, 1, Args) of
     {_, _, P} -> 
       {Pn, _} = string:to_integer(binary_to_list(P)),
       Pn;
             _ -> 6379
  end,
  MaxClients = case lists:keyfind(?MAX_CLIENTS, 1, Args) of
    {_, _, MC} -> 
      {MCn, _} = string:to_integer(binary_to_list(MC)),
      MCn;
             _ -> 5
  end,
  XA = exchange_a(X),

  try 
    case pg2:get_closest_pid(XA) of
      {error, _} -> create_redis_client(XA, Host, Port, MaxClients);
        RedisClient -> 
          case eredis:q(RedisClient, ["PING"]) of
            {ok, <<"PONG">>} -> {ok, RedisClient};
            _    -> 
              error_logger:error_report("Disconnected Redis client discarded."),
              pg2:leave(XA, RedisClient),
              get_redis_client(X)
          end
    end
  catch
    _ -> create_redis_client(XA, Host, Port, MaxClients)
  end.

create_redis_client(XA, Host, Port, MaxClients) ->
  error_logger:info_report(io_lib:format("Starting ~p Redis clients to ~p:~p", [MaxClients, Host, Port])),
  %% Note that it seems possible to specify a "database" when connecting. This
  %% might be useful for a persister!
  case eredis:start_link(Host, Port) of
    {ok, RedisClient} -> 
      pg2:join(XA, RedisClient),
      case length(pg2:get_members(XA)) of
        S when (S < MaxClients) -> create_redis_client(XA, Host, Port, MaxClients);
        _ -> {ok, RedisClient}
      end;
    Err -> Err
  end.

exchange_type(_Exchange=#exchange{ arguments=Args }) ->
%    io:format("Ensuring exchange type doesn't loop: ~w~n", [Exchange]),
  case lists:keyfind(?TYPE, 1, Args) of
    {?TYPE, _, Type} -> 
      io:format("found type ~p~n", [Type]),
      case list_to_atom(binary_to_list(Type)) of
        rabbit_exchange_type_redis -> 
          error_logger:error_report("Cannot base a Redis exchange on a Redis exchange. An infinite loop would occur."),
          rabbit_exchange_type_topic;
        Else -> Else
      end;
    _ -> rabbit_exchange_type_topic
  end.



%% This Source Code Form is subject to the terms of the Mozilla Public
%% License, v. 2.0. If a copy of the MPL was not distributed with this
%% file, You can obtain one at https://mozilla.org/MPL/2.0/.
%%
%% Copyright (c) 2026 Broadcom. All Rights Reserved.

-module(queue_incarnation_comparison_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").
-include_lib("rabbitmq_ct_helpers/include/rabbit_assert.hrl").

-compile(export_all).

all() ->
    [{group, clustered}].

groups() ->
    [{clustered, [], [{group, cluster_size_3},
                     {group, clustered_with_partitions}]},
     {cluster_size_3, [], [stale_delete_preserves_metadata,
                          stale_delete_preserves_bindings,
                          stale_delete_preserves_long_named_queue,
                          delete_after_leader_change,
                          confirmed_messages_survive_restarts]},
     {clustered_with_partitions, [], [recreate_during_partition,
                                     recreate_long_name_during_partition,
                                     cleanup_stale_replica_after_restart,
                                     cleanup_long_name_replica_after_restart]}].

init_per_suite(Config) ->
    Config1 = case os:getenv("COMPARISON_UNIQUE_NAMES") of
                  "true" -> rabbit_ct_helpers:merge_app_env(
                              Config, {rabbit, [{quorum_queue_unique_names, true}]});
                  _ -> Config
              end,
    quorum_queue_SUITE:init_per_suite(Config1).

end_per_suite(Config) ->
    quorum_queue_SUITE:end_per_suite(Config).

init_per_group(clustered_with_partitions = Group, Config) ->
    rabbit_ct_helpers:set_config(
      quorum_queue_SUITE:init_per_group(Group, Config),
      [{comparison_partitioned, true}]);
init_per_group(Group, Config) ->
    quorum_queue_SUITE:init_per_group(Group, Config).

end_per_group(Group, Config) ->
    quorum_queue_SUITE:end_per_group(Group, Config).

init_per_testcase(Testcase, Config) ->
    Config1 = case proplists:get_value(comparison_partitioned, Config, false) of
                  true ->
                      C0 = rabbit_ct_helpers:testcase_started(Config, Testcase),
                      C1 = rabbit_ct_helpers:set_config(
                             C0, [{rmq_nodes_count, 3},
                                  {rmq_nodename_suffix, Testcase},
                                  {tcp_ports_base, {skip_n_nodes, 3}},
                                  {queue_name, atom_to_binary(Testcase)}]),
                      rabbit_ct_helpers:run_steps(
                        C1, rabbit_ct_broker_helpers:setup_steps() ++
                            rabbit_ct_client_helpers:setup_steps());
                  false -> quorum_queue_SUITE:init_per_testcase(Testcase, Config)
              end,
    case Config1 of
        {skip, _} -> Config1;
        _ ->
            case os:getenv("COMPARISON_REQUIRE_DELETING_STATE") of
                "true" ->
                    ?assert(rabbit_ct_broker_helpers:is_feature_flag_enabled(
                              Config1, quorum_queue_deleting_state));
                _ -> ok
            end,
            Config1
    end.

end_per_testcase(Testcase, Config) ->
    Config1 = rabbit_ct_helpers:run_steps(
                Config, rabbit_ct_client_helpers:teardown_steps()),
    Config2 = case proplists:get_value(comparison_partitioned, Config, false) of
                  true -> rabbit_ct_helpers:run_steps(
                            Config1, rabbit_ct_broker_helpers:teardown_steps());
                  false -> Config1
              end,
    rabbit_ct_helpers:testcase_finished(Config2, Testcase).

stale_delete_preserves_metadata(Config) ->
    stale_delete(Config, ?config(queue_name, Config), false).

stale_delete_preserves_bindings(Config) ->
    stale_delete(Config, ?config(queue_name, Config), true).

stale_delete_preserves_long_named_queue(Config) ->
    stale_delete(Config, binary:copy(<<"q">>, 253), true).

stale_delete(Config, QQ, Bind) ->
    [Server, Other | _] = broker_nodes(Config),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    QName = rabbit_misc:r(<<"/">>, queue, QQ),
    declare(Ch, QQ),
    {ok, OldQ} = rpc:call(Server, rabbit_amqqueue, lookup, [QName]),
    delete(Ch, QQ),
    declare(Ch, QQ),
    {ok, NewQ} = rpc:call(Server, rabbit_amqqueue, lookup, [QName]),
    ?assertNotEqual(member_uids(OldQ), member_uids(NewQ)),
    ?awaitMatch({ok, NewQ}, rpc:call(Other, rabbit_amqqueue, lookup, [QName]), 30000),
    case Bind of
        true -> amqp_channel:call(Ch, #'queue.bind'{queue = QQ, exchange = <<"amq.fanout">>});
        false -> ok
    end,
    Bindings = rpc:call(Server, rabbit_db_binding, get_all_for_destination, [QName]),
    ?assertMatch({ok, _}, rpc:call(Other, rabbit_quorum_queue, delete,
                                  [OldQ, false, false, <<"guest">>])),
    ?assertMatch({ok, _}, rpc:call(Server, rabbit_amqqueue, lookup, [QName])),
    ?assertEqual(lists:sort(Bindings), lists:sort(
                   rpc:call(Server, rabbit_db_binding, get_all_for_destination, [QName]))),
    Exchange = case Bind of true -> <<"amq.fanout">>; false -> <<>> end,
    publish_confirmed(Ch, Exchange, QQ, [<<"replacement">>]),
    assert_messages(Ch, QQ, [<<"replacement">>]),
    delete(Ch, QQ).

delete_after_leader_change(Config) ->
    [Server | _] = Servers = broker_nodes(Config),
    QQ = ?config(queue_name, Config),
    QName = rabbit_misc:r(<<"/">>, queue, QQ),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    declare(Ch, QQ),
    {ok, OldQ} = rpc:call(Server, rabbit_amqqueue, lookup, [QName]),
    {RaName, Leader} = amqqueue:get_pid(OldQ),
    [Target | _] = Servers -- [Leader],
    publish_confirmed(Ch, <<>>, QQ, [<<"before leader change">>]),
    ok = rpc:call(Server, ra, transfer_leadership,
                  [{RaName, Leader}, {RaName, Target}]),
    ?awaitMatch({ok, {RaName, Target}}, queue_pid(Server, QName), 30000),
    ?assertMatch({ok, 1}, rpc:call(Server, rabbit_quorum_queue, delete,
                                  [OldQ, false, false, <<"guest">>])),
    ?assertEqual({error, not_found}, rpc:call(Server, rabbit_amqqueue, lookup, [QName])),
    declare(Ch, QQ),
    publish_confirmed(Ch, <<>>, QQ, [<<"after leader change">>]),
    assert_messages(Ch, QQ, [<<"after leader change">>]),
    delete(Ch, QQ).

confirmed_messages_survive_restarts(Config) ->
    [Server | _] = Servers = broker_nodes(Config),
    QQ = ?config(queue_name, Config),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Server),
    declare(Ch, QQ),
    {RaName, _} = queue_pid_value(Server, QQ),
    Payloads = payloads(10),
    publish_confirmed(Ch, <<>>, QQ, Payloads),
    [begin
         ok = rabbit_ct_broker_helpers:restart_node(Config, Node),
         await_leader_convergence(RaName, Servers),
         ?assertMatch({RaName, _}, queue_pid_value(Node, QQ)),
         queue_utils:wait_for_messages_ready(Servers, RaName, 10)
     end || Node <- Servers],
    Ch1 = rabbit_ct_client_helpers:open_channel(Config, Server),
    assert_messages(Ch1, QQ, Payloads),
    delete(Ch1, QQ).

recreate_during_partition(Config) ->
    recreate_during_partition(Config, ?config(queue_name, Config), false).

recreate_long_name_during_partition(Config) ->
    recreate_during_partition(Config, binary:copy(<<"p">>, 253), false).

cleanup_stale_replica_after_restart(Config) ->
    recreate_during_partition(Config, ?config(queue_name, Config), true).

cleanup_long_name_replica_after_restart(Config) ->
    recreate_during_partition(Config, binary:copy(<<"p">>, 253), true).

recreate_during_partition(Config, QQ, Restart) ->
    [Server | _] = Servers = broker_nodes(Config),
    Ch0 = rabbit_ct_client_helpers:open_channel(Config, Server),
    declare(Ch0, QQ),
    {RaName, _} = queue_pid_value(Server, QQ),
    %% A stale replica with a higher term can overwrite the replacement log.
    [elect(RaName, Node) || _ <- lists:seq(1, 2), Node <- Servers],
    {ok, _, {_, Leader}} = rpc:call(Server, ra, members, [{RaName, Server}]),
    [Stale, Follower] = Servers -- [Leader],
    ChOld = rabbit_ct_client_helpers:open_channel(Config, Stale),
    publish_confirmed(ChOld, <<>>, QQ, [<<"old incarnation">>]),
    queue_utils:wait_for_messages_ready(Servers, RaName, 1),
    OldUId = rpc:call(Stale, ra_directory, uid_of, [quorum_queues, RaName]),
    ?assert(is_binary(OldUId)),
    rabbit_ct_broker_helpers:block_traffic_between(Stale, Leader),
    rabbit_ct_broker_helpers:block_traffic_between(Stale, Follower),
    await_disconnected(Stale, Leader),
    await_disconnected(Stale, Follower),
    Ch = rabbit_ct_client_helpers:open_channel(Config, Leader),
    delete(Ch, QQ),
    declare(Ch, QQ),
    {NewRaName, _} = queue_pid_value(Leader, QQ),
    Payloads = payloads(10),
    publish_confirmed(Ch, <<>>, QQ, Payloads),
    queue_utils:wait_for_messages_ready([Leader, Follower], NewRaName, 10),
    ct:pal("Incarnation names before healing: old=~tp replacement=~tp", [RaName, NewRaName]),
    rabbit_ct_broker_helpers:allow_traffic_between(Stale, Follower),
    await_connected(Stale, Follower),
    Election = rpc:call(Stale, ra, trigger_election, [{RaName, Stale}]),
    ct:pal("Stale replica election result: ~tp", [Election]),
    rabbit_ct_broker_helpers:allow_traffic_between(Stale, Leader),
    await_connected(Stale, Leader),
    ConvergenceNodes = case NewRaName of
                           RaName -> Servers;
                           _ -> [Leader, Follower]
                       end,
    await_leader_convergence(NewRaName, ConvergenceNodes),
    {ok, {_, Count}, _} = rpc:call(Follower, ra, leader_query,
                                  [{NewRaName, Follower},
                                   {rabbit_fifo, query_messages_ready, []}]),
    ct:pal("Confirmed replacement messages after healing: ~b", [Count]),
    ?assertEqual(10, Count),
    ct:pal("Old replica UID before restart: ~tp", [
             rpc:call(Stale, ra_directory, uid_of, [quorum_queues, RaName])]),
    case Restart of
        true ->
            ok = rabbit_ct_broker_helpers:restart_node(Config, Stale),
            await_leader_convergence(NewRaName, Servers),
            queue_utils:wait_for_messages_ready(Servers, NewRaName, 10),
            RemainingUId = rpc:call(Stale, ra_directory, uid_of, [quorum_queues, RaName]),
            ct:pal("Old replica UID after restart: ~tp", [RemainingUId]),
            ?assertNotEqual(OldUId, RemainingUId);
        false -> ok
    end,
    assert_messages(Ch, QQ, Payloads),
    delete(Ch, QQ).

broker_nodes(Config) ->
    rabbit_ct_broker_helpers:get_node_configs(Config, nodename).

member_uids(Q) ->
    maps:get(nodes, amqqueue:get_type_state(Q)).

declare(Ch, QQ) ->
    ?assertEqual({'queue.declare_ok', QQ, 0, 0}, amqp_channel:call(
                   Ch, #'queue.declare'{queue = QQ, durable = true,
                                       arguments = [{<<"x-queue-type">>, longstr, <<"quorum">>}]})).

delete(Ch, QQ) ->
    ?assertMatch(#'queue.delete_ok'{}, amqp_channel:call(Ch, #'queue.delete'{queue = QQ})).

queue_pid_value(Server, QQ) ->
    {ok, Pid} = queue_pid(Server, rabbit_misc:r(<<"/">>, queue, QQ)),
    Pid.

queue_pid(Server, QName) ->
    case rpc:call(Server, rabbit_amqqueue, lookup, [QName]) of
        {ok, Q} -> {ok, amqqueue:get_pid(Q)};
        Error -> Error
    end.

payloads(Count) ->
    [<<"replacement-", (integer_to_binary(N))/binary>> || N <- lists:seq(1, Count)].

publish_confirmed(Ch, Exchange, QQ, Payloads) ->
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    [amqp_channel:cast(Ch, #'basic.publish'{exchange = Exchange, routing_key = QQ},
                       #amqp_msg{props = #'P_basic'{delivery_mode = 2}, payload = P})
     || P <- Payloads],
    ?assertEqual(true, amqp_channel:wait_for_confirms(Ch, 10)).

assert_messages(Ch, QQ, Payloads) ->
    Actual = [begin
                  {#'basic.get_ok'{}, #amqp_msg{payload = P}} = amqp_channel:call(
                    Ch, #'basic.get'{queue = QQ, no_ack = true}),
                  P
              end || _ <- Payloads],
    ?assertEqual(lists:sort(Payloads), lists:sort(Actual)),
    ?assertMatch(#'basic.get_empty'{}, amqp_channel:call(Ch, #'basic.get'{queue = QQ, no_ack = true})).

elect(RaName, Node) ->
    ServerId = {RaName, Node},
    ?awaitMatch({ok, _, ServerId},
                case rpc:call(Node, ra, members, [ServerId]) of
                    {ok, _, ServerId} = Members -> Members;
                    Other ->
                        _ = rpc:call(Node, ra, trigger_election, [ServerId]),
                        Other
                end, 30000).

await_leader_convergence(RaName, Servers) ->
    rabbit_ct_helpers:await_condition(
      fun() ->
              Leaders = [Leader || S <- Servers,
                                   {ok, _, Leader} <- [rpc:call(S, ra, members, [{RaName, S}, 1000])]],
              length(Leaders) =:= length(Servers) andalso length(lists:usort(Leaders)) =:= 1
      end, 30000).

await_disconnected(Node, Peer) ->
    rabbit_ct_helpers:await_condition(
      fun() -> not lists:member(Peer, erpc:call(Node, erlang, nodes, [])) end, 30000).

await_connected(Node, Peer) ->
    rabbit_ct_helpers:await_condition(
      fun() -> true =:= erpc:call(Node, net_kernel, connect_node, [Peer]) end, 30000).

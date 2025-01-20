import argparse
import importlib
from pathlib import Path
import yaml
from asyncio import run
from ipv8.configuration import ConfigBuilder, Strategy, WalkerDefinition, default_bootstrap_defs, BootstrapperDefinition, Bootstrapper
from ipv8.util import create_event_with_signals
from ipv8_service import IPv8

def load_algorithm(alg_name: str, location = 'cs4545'):
    try:
        mod = importlib.import_module(f'{location}.implementation')
        return getattr(mod, 'get_algorithm')(alg_name)
    except ModuleNotFoundError as e:
        print(f'No external algorithms found in {location}')
        raise e


async def start_communities(node_id, connections, algorithm, \
            use_localhost=True, starting_nodes=[], broadcast_num=0, byzatine_num=0, nodes_num=10, \
                is_byzantine=False, byzatine_behaviour = "ignore_msg", \
                is_brb_byzantine=False, brb_byzantine_num=0,\
                MD1=False, MD2=False, MD3=False, MD4=False, MD5=False) -> None:
    event = create_event_with_signals()
    base_port = 9090
    connections_updated = [(x, base_port + x) for x in connections]
    node_port = base_port + node_id
    builder = ConfigBuilder().clear_keys().clear_overlays()
    builder.add_key("my peer", "medium", f"ec{node_id}.pem")
    builder.set_port(node_port)
    builder.add_overlay(
        "DA_Alg_Test",
        "my peer",
        # [WalkerDefinition(Strategy.RandomWalk,
        #                                       10, {'timeout': 3.0})],
        # default_bootstrap_defs,
        # default_bootstrap_defs,
        # [BootstrapperDefinition(Bootstrapper.DispersyBootstrapper,
        #                                             {'ip_addresses': [],
        #                                              'dns_addresses': []})],
        [],
        [],
        {},
        [("started", node_id, connections_updated, event, \
                use_localhost, starting_nodes, broadcast_num, byzatine_num, nodes_num, is_byzantine, byzatine_behaviour, \
                is_brb_byzantine, brb_byzantine_num,\
                        MD1, MD2, MD3, MD4, MD5)],
    )
    ipv8_instance = IPv8(
        builder.finalize(), extra_communities={"DA_Alg_Test": algorithm}
    )
    await ipv8_instance.start()
    await event.wait()
    await ipv8_instance.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="Distributed Algorithms",
        description="Code to execute distributed algorithms.",
        epilog="written by Bart Cox (2023)",
    )
    parser.add_argument("node_id", type=int)
    parser.add_argument("is_broadcaster", type=str, default="false")
    parser.add_argument("broadcast_num", help="how many msg should this broadcaster send", type=int, default=0)
    parser.add_argument("is_byzantine", type=str, default="false")
    parser.add_argument("byzatine_num", help="aka f", type=int, default=0)
    parser.add_argument("nodes_num", help="aka N", type=int, default=10)
    parser.add_argument("byzatine_behaviour", type=str, default="ignore_msg")
    parser.add_argument("is_brb_byzantine", type=str, default="false")
    parser.add_argument("brb_byzatine_num", help="aka f", type=int, default=0)
    parser.add_argument("topology", type=str, nargs="?", default="topologies/default.yaml")
    parser.add_argument("algorithm", type=str, nargs="?", default='causal')
    parser.add_argument("-location", type=str, default='cs4545')
    parser.add_argument("-docker", action='store_true')
    parser.add_argument("-MD1", action='store_true')
    parser.add_argument("-MD2", action='store_true')
    parser.add_argument("-MD3", action='store_true')
    parser.add_argument("-MD4", action='store_true')
    parser.add_argument("-MD5", action='store_true')
    args = parser.parse_args()
    node_id = args.node_id
    is_broadcaster = args.is_broadcaster == "true"
    is_byzantine = args.is_byzantine == "true"
    is_brb_byzantine = args.is_brb_byzantine == "true"
    starting_nodes = []
    if is_broadcaster:
        starting_nodes = [node_id]

    # alg = get_algorithm(args.algorithm)
    alg = load_algorithm(args.algorithm, location=args.location)
    with open(args.topology, "r") as f:
        topology = yaml.safe_load(f)
        connections = topology[node_id]

        run(start_communities(node_id, connections, alg, not args.docker,\
                               starting_nodes, args.broadcast_num, \
                                args.byzatine_num, args.nodes_num, is_byzantine, args.byzatine_behaviour, \
                                is_brb_byzantine, args.brb_byzatine_num, \
                                args.MD1, args.MD2, args.MD3, args.MD4, args.MD5))

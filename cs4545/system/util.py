import copy
from pathlib import Path
import random
from typing import Optional

import click
import yaml


@click.group()
def cli():
    pass

@cli.command('compose_dolev')
@click.argument('num_nodes', type=int)
@click.argument('broadcaster_num', type=int)
@click.argument('broadcast_num_per_node', type=int)
@click.argument('byzantine_num', type=int)
@click.argument('topology_file', type=str, default='topologies/dolev.yaml')
@click.option('--template_file', type=str, default='docker-compose.template.yml')
def compose_dolev(num_nodes, broadcaster_num, broadcast_num_per_node, byzantine_num, topology_file, template_file):
    prepare_compose_dolev_file(num_nodes, broadcaster_num, broadcast_num_per_node, byzantine_num, topology_file, template_file)

@cli.command('compose')
@click.argument('num_nodes', type=int)
@click.argument('topology_file', type=str, default='topologies/echo.yaml')
@click.argument('algorithm', type=str, default='echo')
@click.option('--template_file', type=str, default='docker-compose.template.yml')
def compose(num_nodes, topology_file, algorithm, template_file):
    prepare_compose_file(num_nodes, topology_file, algorithm, template_file)


def generate_2f_plus_1_connected_graph(num_nodes, f):
    import networkx as nx
    import matplotlib.pyplot as plt
    k = 2 * f + 1  # Required degree and connectivity

    # Ensure that the degree is less than the number of nodes
    if k >= num_nodes:
        raise ValueError("Number of nodes must be greater than 2f + 1.")

    if k >= num_nodes // 2:
        G = nx.complete_graph(num_nodes)
        return { node: list(G.neighbors(node)) for node in G.nodes() }
    else:
        while True:
            try:
                # Try to generate a random k-regular graph
                G = nx.random_regular_graph(k, num_nodes)
            except nx.NetworkXError as e:
                print(f"Error generating {k}-regular graph: {e}")
                G = nx.complete_graph(num_nodes)
                return { node: list(G.neighbors(node)) for node in G.nodes() }
            else:
                # Check if the graph is (2f + 1)-connected
                connectivity = nx.node_connectivity(G)
                if connectivity >= k:
                    print(f"Generated a {k}-connected random regular graph with {num_nodes} nodes.")
                    connections = { node: list(G.neighbors(node)) for node in G.nodes() }
                    return connections
                else:
                    print(f"Graph connectivity {connectivity} is less than required {k}, regenerating...")

def prepare_compose_dolev_file(num_nodes, broadcaster_num, broadcast_num_per_node, byzantine_num, topology_file, template_file):
    with open(template_file, 'r') as f:
        content = yaml.safe_load(f)

        node = content['services']['node0']
        content['x-common-variables']['TOPOLOGY'] = topology_file

        nodes = {}
        baseport = 9090

        network_name = list(content['networks'].keys())[0]
        subnet = content['networks'][network_name]['ipam']['config'][0]['subnet'].split('/')[0]
        network_base = '.'.join(subnet.split('/')[0].split('.')[:-1])

        rem_broadcaster_num = broadcaster_num
        rem_byzantine_num = byzantine_num


        # Create a f + 1 vertex degree topology
        for i in range(num_nodes):
            n = copy.deepcopy(node)
            n['ports'] = [f'{baseport + i}:{baseport + i}']
            n['networks'][network_name]['ipv4_address'] = f'{network_base}.{10 + i}'
            n['environment']['PID'] = i
            flag_broadcaster = random.randint(1, num_nodes - i) <= rem_broadcaster_num
            if flag_broadcaster: rem_broadcaster_num -= 1
            n['environment']['IS_BROADCASTER'] = flag_broadcaster
            n['environment']['BROADCAST_NUM'] = broadcast_num_per_node

            flag_byzantine = random.randint(1, num_nodes - i) <= rem_byzantine_num and not flag_broadcaster
            if flag_byzantine: rem_byzantine_num -= 1
            n['environment']['IS_BYZANTINE'] = flag_byzantine
            n['environment']['BYZANTINE_NUM'] = byzantine_num
            n['environment']['BYZANTINE_BEHAVIOUR'] = "IGNORE_MSG"

            n['environment']['TOPOLOGY'] = topology_file
            n['environment']['ALGORITHM'] = "dolev"
            n['environment']['LOCATION'] = "cs4545"
            nodes[f'node{i}'] = n



        connections = generate_2f_plus_1_connected_graph(num_nodes, byzantine_num)

        content['services'] = nodes

        with open('docker-compose.yml', 'w') as f2:
            yaml.safe_dump(content, f2)
            print(f'Output written to docker-compose.yml')

        with open(topology_file, 'w') as f3:
            yaml.safe_dump(connections, f3)
            print(f'Output written to {topology_file}')


def prepare_compose_file(num_nodes, topology_file, algorithm, template_file, location='cs4545'):
    with open(template_file, 'r') as f:
        content = yaml.safe_load(f)

        node = content['services']['node0']
        content['x-common-variables']['TOPOLOGY'] = topology_file

        nodes = {}
        baseport = 9090
        connections = {}

        network_name = list(content['networks'].keys())[0]
        subnet = content['networks'][network_name]['ipam']['config'][0]['subnet'].split('/')[0]
        network_base = '.'.join(subnet.split('/')[0].split('.')[:-1])

        # Create a ring topology
        for i in range(num_nodes):
            n = copy.deepcopy(node)
            n['ports'] = [f'{baseport + i}:{baseport + i}']
            n['networks'][network_name]['ipv4_address'] = f'{network_base}.{10 + i}'
            n['environment']['PID'] = i
            n['environment']['TOPOLOGY'] = topology_file
            n['environment']['ALGORITHM'] = algorithm
            n['environment']['LOCATION'] = location
            nodes[f'node{i}'] = n

            connections[i] = [(i + 1) % num_nodes, (i - 1) % num_nodes]

        content['services'] = nodes

        with open('docker-compose.yml', 'w') as f2:
            yaml.safe_dump(content, f2)
            print(f'Output written to docker-compose.yml')

        with open(topology_file, 'w') as f3:
            yaml.safe_dump(connections, f3)
            print(f'Output written to {topology_file}')


@cli.command('cfg')
@click.argument('cfg_file', type=str)
def prepare_from_cfg(cfg_file: str):
    with open(cfg_file, 'r') as f:
        cfg = yaml.safe_load(f)
        # print(cfg)
        if 'template' not in cfg:
            cfg['template'] = 'docker-compose.template.yml'
        if 'location' not in cfg:
            cfg['location'] = 'cs4545'
        prepare_compose_file(cfg['num_nodes'], cfg['topology'], cfg['algorithm'], cfg['template'], cfg['location'])


@cli.command()
@click.argument('cfg_file', type=str)
@click.argument('output_dir', type=str)
@click.option('--verbose', type=bool, default=True)
@click.option('--append_file', type=str)
@click.option('--name', type=str)
def eval(cfg_file: str, output_dir: str, verbose: bool = True, append_file: Optional[str] = None,
         name: Optional[str] = None):
    if verbose:
        print('Evaluating output')
    with open(cfg_file, 'r') as f:
        cfg = yaml.safe_load(f)

    out_dir = Path(output_dir)
    out_files = {}
    for f in [x for x in out_dir.iterdir() if x.suffix == '.out']:
        with open(f, 'r') as f2:
            # Load the txt in the file
            out_files[f.stem] = [x.rstrip() for x in f2.readlines()]
    valid = 0
    invalid = 0

    node_stats = [yaml.safe_load(open(x)) for x in out_dir.iterdir() if x.suffix == '.yml']

    # Aggregate the node stats where the structure is a list of dictionaries with the same keys
    agg_stats = {}
    for key in node_stats[0].keys():
        agg_stats[key] = [x[key] for x in node_stats]
        try:
            agg_stats[key] = sum(agg_stats[key])
        except Exception:
            pass
    agg_stats['num_nodes'] = len(node_stats)
    agg_stats['algorithm'] = cfg['algorithm']

    if 'expected_output' not in cfg:
        print('No expected output found in cfg')
    else:
        for node_name in cfg['expected_output']:
            node_output = iter(out_files[node_name])
            eval_output = cfg['expected_output'][node_name]

            for expected_val in eval_output:
                try:
                    node_val = next(node_output)
                    if expected_val != node_val:
                        if verbose:
                            print(f'Output mismatch for {node_name} at {expected_val} != {node_val}')
                        invalid += 1
                    else:
                        valid += 1
                except StopIteration:
                    if verbose:
                        print('Output mismatch: Expected more output')
                    invalid += 1

    if valid + invalid == 0:
        score = 0.0
    else:
        score = (valid / float(valid + invalid)) * 100.0
    if verbose:
        print(f'Valid: {valid} Invalid: {invalid}, Score: {score:.2f}%')

        print(agg_stats)
    if append_file and name:
        print(f'Appending to {append_file} for {name}')
        csv_line = ','.join([name, str(valid), str(invalid), f'{score:.2f}'])
        with open(append_file, 'a') as f:
            f.write(csv_line)
            f.write('\n')


@cli.command("d")
@click.argument('topology_file', type=str)
def draw_topology(topology_file: str):
    with open(topology_file, 'r') as f:
        edges = yaml.safe_load(f)
        import networkx as nx
        import matplotlib.pyplot as plt
        G = nx.DiGraph()
        for node, connections in edges.items():
            for conn in connections:
                G.add_edge(f'{node}', f'{conn}')
        pos = nx.spring_layout(G)
        nx.draw(G, with_labels=True, pos=pos)
        plt.show()


@cli.command("d2")
@click.argument('topology_file', type=str)
def draw_topology2(topology_file: str, yaml_file: str="docker-compose.yml"):
    with open(topology_file, 'r') as f:
        edges = yaml.safe_load(f)
    with open(yaml_file, 'r') as f2:
        config = yaml.safe_load(f2)
    
    import networkx as nx
    import matplotlib.pyplot as plt

    G = nx.DiGraph()

    for node, connections in edges.items():
        for conn in connections:
            G.add_edge(f'{node}', f'{conn}')
    
    node_colors = []
    for node in G.nodes:

        is_broadcaster = False
        is_byzantine = False
        for nodes in config['services'].values():

            print(node, nodes['environment']['PID'])
            if int(node) == int(nodes['environment']['PID']):
                is_broadcaster = nodes['environment']['IS_BROADCASTER']
                is_byzantine = nodes['environment']['IS_BYZANTINE']
        
        if is_broadcaster and is_byzantine:
            node_colors.append('orange')
        elif is_broadcaster:
            node_colors.append('green') 
        elif is_byzantine:
            node_colors.append('red')
        else:
            node_colors.append('gray') 
    
    
    pos = nx.spring_layout(G)
    nx.draw(G, with_labels=True, pos=pos, node_color=node_colors, edge_color='black', node_size=700, font_size=10, font_color='black')
    plt.show()


if __name__ == '__main__':
    cli()

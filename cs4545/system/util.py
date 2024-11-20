import copy
from pathlib import Path
import random
from typing import Optional

import click
import yaml
import os


@click.group()
def cli():
    pass

@cli.command('report')
@click.argument('num_nodes', type=int)
@click.argument('num_msg', type=int)
@click.argument('output_folder', type=str, default='output')
def report(num_nodes,num_msg,output_folder):
    total_sent_msg_count = 0
    delivered_times = []
    all_node_delivered = True
    average_deliver_time_list = []
    
    headers = ["Node ID", "All Delivered", "Delivered Count", "Meg Send Count", "Time Spent", "Avg Time"]
    col_widths = [10, 18, 18, 18, 18, 18]
    header_line = "".join(f"{header:<{width}}" for header, width in zip(headers, col_widths))
    print(header_line)
    print("=" * sum(col_widths))

    # Loop through the range of file indices based on n
    for i in range(num_nodes):
        file_name = f"node-{i}.yml"
        file_path = os.path.join(output_folder, file_name)
        
        # Check if the file exists
        if os.path.exists(file_path):
            with open(file_path, 'r') as file:
                data = yaml.safe_load(file)
                
                is_byzantine = data.get("is_byzantine", False)
                
                # Extract the records
                records = data.get("records", {})
                
                # Find the last delivered time if all records are delivered
                time_list = [record.get("time_spent", 0) for record in records.values() if record.get("time_spent") is not None]
                last_delivered_time = max(time_list,default=0)
                average_deliver_time = sum(time_list)/len(time_list)  if time_list else 0
                delivered_times.append(last_delivered_time)
                
                if not is_byzantine:
                    average_deliver_time_list.append(average_deliver_time)

                
                # Extract relevant data
                node_sent_msg_count = data.get("total_sent_msg_count", 0)
                node_delivered_msg = data.get("total_delivered_msg", 0)
                total_sent_msg_count += node_sent_msg_count
                
                all_delivered = num_msg == node_delivered_msg
                
                if(not is_byzantine and not all_delivered):
                    all_node_delivered = False
                
                node_simple = str(i) + ("*" if is_byzantine else "")
                print(
                    f"{node_simple:<10}"  # Msg ID
                    f"{str(all_delivered):<18}"  # Delivered
                    f"{node_delivered_msg:<18}"  # Recv Count
                    f"{node_sent_msg_count:<18}"  # Send Count
                    f"{last_delivered_time:<18.4f}"  # Time Spent
                    f"{average_deliver_time:<18.4f}"  # Time Spent
                )
        else:
            print(f"File {file_name} not found. Skipping.")
        
    overal_avg_time = sum(average_deliver_time_list) / len(average_deliver_time_list)
    print("\n")
    print(f"All good = [{all_node_delivered}], Total msg sent: {total_sent_msg_count}, Last Delivered Time: {max(delivered_times)}, AVG Time: {overal_avg_time}")

@cli.command('compose_dolev')
@click.argument('num_nodes', type=int)
@click.argument('broadcaster_num', type=int)
@click.argument('broadcast_num_per_node', type=int)
@click.argument('byzantine_num', type=int)
@click.argument('vertex_degree', type=int)
@click.argument('topology_file', type=str, default='topologies/dolev.yaml')
@click.option('--template_file', type=str, default='docker-compose.template.yml')
def compose_dolev(num_nodes, broadcaster_num, broadcast_num_per_node, byzantine_num,\
                        vertex_degree, topology_file, template_file):
    prepare_compose_dolev_file(num_nodes, broadcaster_num, broadcast_num_per_node, byzantine_num,\
                               vertex_degree, topology_file, template_file)

@cli.command('compose')
@click.argument('num_nodes', type=int)
@click.argument('topology_file', type=str, default='topologies/echo.yaml')
@click.argument('algorithm', type=str, default='echo')
@click.option('--template_file', type=str, default='docker-compose.template.yml')
def compose(num_nodes, topology_file, algorithm, template_file):
    prepare_compose_file(num_nodes, topology_file, algorithm, template_file)


def generate_connected_graph(num_nodes, vertex_degree):
    import networkx as nx
    import matplotlib.pyplot as plt
    k = vertex_degree  # Required degree and connectivity

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

def prepare_compose_dolev_file(num_nodes, \
                               broadcaster_num, broadcast_num_per_node, \
                                byzantine_num, \
                                vertex_degree, \
                                topology_file, template_file):
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

        connections = generate_connected_graph(num_nodes, vertex_degree)

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

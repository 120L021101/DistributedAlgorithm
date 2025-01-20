from typing import List
from cs4545.system.da_types import *
import networkx as nx
import os

@dataclass(msg_id=4)  # The value 1 identifies this message and must be unique per community.
class DolevMessage:
    source_id : int
    sender_id : int
    message : str
    message_id : str
    path : List[int]

class DolevAlgorithm(DistributedAlgorithm):
    """_summary_
    Assignment 1, reliable communication, dolev algorithm
    Args:
        DistributedAlgorithm (_type_): _description_
    """
    
    @staticmethod
    def path_to_bitmask(path):
        bitmask = 0
        for node in path:
            bitmask |= (1 << node)
        return bitmask
        
    @staticmethod
    def formatPath(path):
        nodes = [i for i in range(path.bit_length()) if (path >> i) & 1]
        return '[' + ', '.join(map(str, nodes)) + ']'

    @staticmethod
    def formatPaths(paths):
        return '[' + ', '.join([
            str(DolevAlgorithm.formatPath(path))
            for path in paths
        ]) + ']'

    # upon event ⟨Dolev, Init⟩ 
    def __init__(self, settings: CommunitySettings) -> None:
        # node_id, neighbors etc, all in this "settings"
        super().__init__(settings)

        self.delivered = dict()
        self.log_reason = dict()
        self.delivered_neighbors = dict()
        self.paths = dict()
        self.sent_msg_cnt = 0
        self.seq_id = 0
        self.byzantined_msg_id = []
        
        # 方便测试，针对latency和msg复杂度
        self.interface_recv_msg_cnt = 0
        self.interface_sent_msg_cnt = 0
        self.protocol_delivered_msg_cnt = 0

        self.out_dolev = open(os.devnull, mode='w', encoding='utf-8')
        
        self.add_message_handler(DolevMessage, self.al_deliver)

    async def on_start(self):
        # Make sure to call this one last in this function
        self.start_time = time.time()
        await super().on_start()
    
    async def dolev_broadcast(self, message=None):
        # 如果是malicious broadcaster，只广播f个邻居，否则有多少广播多少
        broadcast_neighbor_upper_bound = self.f if self.is_byzantine else len(self.get_peers())

        self.start_time = time.time()
        seq_id = self.seq_id
        self.seq_id += 1 
        if message is None:
            message = f"Hello From {self.node_id}"
        for i in range(self.broadcast_num):
            message_id = f"{self.node_id}:{seq_id}"
            self.sent_msg_cnt += 1
            sent_payload = DolevMessage(self.node_id,self.node_id, message, message_id, [])
            for n_id, peer in enumerate(self.get_peers()):
                if n_id == broadcast_neighbor_upper_bound:
                    break
                # broadcast to all neighbors  
                peer_id = self.node_id_from_peer(peer=peer)
                print(f"[Node {self.node_id}] Broadcast to {peer_id}", file=self.out_dolev)  
                await self.send_with_delay(peer, "", sent_payload)
                self.logger.sent_msg(message_id)

            self.delivered[message_id] = True
            self.protocol_delivered_msg_cnt += 1
            # Delivered!
            await self.dolev_deliver(sent_payload)
            print(f"[Node {self.node_id}] Delivered Message: [{message_id}]:{message}", file=self.out_dolev)
            self.logger.log_delivery(message_id, "As Source")
            self.logger.write_log_to_output()

    # upon event ⟨Dolev, Broadcast | m⟩
    # only starter does "Broadcast"
    async def on_start_as_starter(self, message=None):
        print(f"[Node {self.node_id}] is starting the Delov algorithm", file=self.out_dolev)
        await self.dolev_broadcast(message=message)
        
    
    def extract_disjoint_paths(self, flow_dict, source, sink):

        disjoint_paths = []
        visited_edges = set()  # Track edges that have been used

        while True:
            path = []
            current_node = source
            while current_node != sink:
                # Merge `_in` and `_out` into the base node name
                if type(current_node) == str:
                    base_node = current_node.replace("_in", "").replace("_out", "")
                else:
                    base_node = current_node
                if not path or path[-1] != base_node:  # Avoid duplicate nodes in the path
                    path.append(base_node)

                found_next = False
                for neighbor, flow in flow_dict.get(current_node, {}).items():
                    # Only follow forward edges with positive flow
                    edge = (current_node, neighbor)
                    if flow > 0 and edge not in visited_edges:
                        visited_edges.add(edge)  # Mark edge as used
                        flow_dict[current_node][neighbor] -= 1  # Reduce flow for this edge
                        current_node = neighbor
                        found_next = True
                        break
                if not found_next:  # No valid path from current_node
                    return disjoint_paths
            # Add the sink to the path
            if type(sink) == str:
                base_sink = sink.replace("_in", "").replace("_out", "")
            else:
                base_sink = sink
            path.append(base_sink)
            disjoint_paths.append(path)

    def has_exact_f_plus_one_vertex_disjoint_paths(self, pathsets, source, sink):
        """
        Determines if there are exactly f + 1 vertex-disjoint paths between source and sink
        using pathsets represented as bitmasks.

        Parameters:
            pathsets (list of int): Each bitmask represents a path. Only contains nodes between source and sink.
            source (int): Source node.
            sink (int): Sink node.
            f (int): Fault tolerance level.

        Returns:
            bool: True if there are exactly f + 1 vertex-disjoint paths, False otherwise.
        """    
        print(f"[Node {self.node_id}] check disjoint: {pathsets}", file=self.out_dolev)
        nodes_in_paths = set()
        for path in pathsets:
            nodes_in_paths.update(path)
        
        # Add source and sink explicitly (even if they're not in the pathsets)
        nodes_in_paths.add(source)
        nodes_in_paths.add(sink)
        
        
        # Create a directed graph from the pathsets
        flow_network = nx.DiGraph()
        for nodes_in_path in pathsets:
            for u, v in zip(nodes_in_path, nodes_in_path[1:]):  # Sequential pairs
                if not flow_network.has_edge(u, v):
                    flow_network.add_edge(u, v, capacity=1)
        
        # Connect source to the start of paths
        for path in pathsets:
            if len(path) == 0:
                continue
            if path[0] != source:  # Ensure no duplicate edges
                flow_network.add_edge(source, path[0], capacity=1)

        # Connect end of paths to the sink
        for path in pathsets:
            if len(path) == 0:
                continue
            if path[-1] != sink:  # Ensure no duplicate edges
                flow_network.add_edge(path[-1], sink, capacity=1)
        
        # Node splitting for vertex capacity constraints
        split_flow_network = nx.DiGraph()
        for node in nodes_in_paths:
            if node == source or node == sink:
                continue
            # Add in-node and out-node for each node
            in_node = f"{node}_in"
            out_node = f"{node}_out"
            split_flow_network.add_node(in_node)
            split_flow_network.add_node(out_node)
            # Edge from in-node to out-node with capacity 1
            split_flow_network.add_edge(in_node, out_node, capacity=1)
        
        # Add edges from the original graph to the split graph
        for u, v in flow_network.edges():
            # Handle source and sink separately
            if u == source:
                u_node = source
            else:
                u_node = f"{u}_out"
            if v == sink:
                v_node = sink
            else:
                v_node = f"{v}_in"
            split_flow_network.add_edge(u_node, v_node, capacity=1)
        
        # Add source and sink nodes to the split graph
        split_flow_network.add_node(source)
        split_flow_network.add_node(sink)
        
        # Compute maximum flow
        flow_value, flow_dict = nx.maximum_flow(split_flow_network, source, sink)
    
        disjoint_path = self.extract_disjoint_paths(flow_dict, source, sink)
        
        # Check if the maximum flow equals f + 1
        return flow_value >= self.f + 1, disjoint_path
    
    def __dfs_max_disjoint(self, idx, cur_picks, bit_paths, max_val) -> int:
        if idx == len(cur_picks):
            if sum(cur_picks) != self.f + 1:
                return max_val
            for i, cur_pick in enumerate(cur_picks):
                for j, cur_pick2 in enumerate(cur_picks):
                    if i == j: continue
                    if cur_pick == 0 or cur_pick2 == 0: continue
                    if bit_paths[i] & bit_paths[j] != 0:
                        return max_val
            return max(max_val, sum(cur_picks))
        cur_picks[idx] = 0
        max_val = self.__dfs_max_disjoint(idx + 1, cur_picks, bit_paths, max_val)
        cur_picks[idx] = 1
        max_val = self.__dfs_max_disjoint(idx + 1, cur_picks, bit_paths, max_val)
        return max_val
        
    
    def criteria(self, bit_paths: set) -> bool:
        # check if exists exactly f + 1 vertex disjoint paths
        print(f"[Node {self.node_id}] check disjoint: {self.formatPaths(bit_paths)}", file=self.out_dolev)
        if(len(bit_paths) < self.f +1):
            return False
        bit_paths = list(bit_paths)
        return self.__dfs_max_disjoint(0, [0 for _ in range(len(bit_paths))],
                                       bit_paths, 0) >= self.f + 1
    
    def ignore_msg(self, payload):
        # byzantine: ignore msg, do nothing
        print(f"[Node {self.node_id}] ignores {payload.message_id} : {payload.message}", file=self.out_dolev)
        return False 
    
    async def modify_msg_id(self, payload):
        # byzantine: increase source id by 10
        if(len(self.byzantined_msg_id) >= 2 or payload.message_id in self.byzantined_msg_id):
            return False
        self.byzantined_msg_id.append(payload.message_id)
        pj = payload.source_id
        payload.source_id = self.get_modify_source_id(pj, self.node_id)
        print(f"[Node {self.node_id}] modify {payload.message_id}'s source_id from {pj} to {payload.source_id}", file=self.out_dolev)
        payload.message_id = f"{payload.source_id}" + ":" + payload.message_id.split(":")[1]
        print(f"[Node {payload.source_id}] modify message id to {payload.message_id}", file=self.out_dolev)
        
        for neighbor in self.get_peers():
            neighbor_id = self.node_id_from_peer(peer=neighbor)
            print(f"[Node {self.node_id}] send fake msg {payload.message_id} to {neighbor_id}", file=self.out_dolev)
            await self.send_with_delay(neighbor, "", DolevMessage(payload.source_id, self.node_id, payload.message, payload.message_id, payload.path))
            self.logger.sent_msg(payload.message_id)
        return False
    
    def get_modify_source_id(self, source_id, node_id):
        while True:
            if source_id != 0:
                source_id -= 1
            else:
                source_id += 1
            if(source_id != node_id):
                return source_id

    async def dolev_deliver(self, payload):
        return

    # upon event ⟨al, Deliver | pj , [m, path]⟩
    @message_wrapper(DolevMessage)
    async def al_deliver(self, neighbor: Peer, payload: DolevMessage) -> None:
        # if is non broadcaster byzantine node, act based on byzantine behaviour
        if self.is_byzantine and (self.node_id not in self.starting_nodes):
            (behaviour, args) = {
                # 忽略消息
                "IGNORE_MSG": (self.ignore_msg, (payload, )),
                # 修改消息的origin id，消息源
                "MODIFY_MSG_ID": (self.modify_msg_id, (payload,)),
            }.get(self.byzantine_behaviour, (None, None))

            if behaviour is None:
                return

            # log
            self.logger.write_log_to_output()

            # e.g. if ignore, return, if modify, continue
            if self.byzantine_behaviour == "IGNORE_MSG":
                if_continue = behaviour(*args)
            elif self.byzantine_behaviour == "MODIFY_MSG_ID":
                if_continue = await behaviour(*args)
            if not if_continue:
                return
            
        receiver_id = self.node_id
        sender_id, m, m_id, received_path, source_id = payload.sender_id, payload.message, payload.message_id, payload.path, payload.source_id
        received_path_bitmask = self.path_to_bitmask(received_path)
        seq_id = self.seq_id
        self.seq_id += 1
        
            
        # initialize variables if needed
        if m_id not in self.paths:
            self.paths[m_id] = set()
        if m_id not in self.delivered:
            self.delivered[m_id] = False
            self.log_reason[m_id] = ""
            self.delivered_neighbors[m_id] = 0
        print(f"[Node {receiver_id}:{seq_id}] Got a message {m} source of {source_id} with sender {sender_id}.\t" + 
                f"msg_id: {m_id}, msg path: {received_path} " +
                                 f"and local paths: {self.paths[m_id]}", file=self.out_dolev)
        self.logger.recv_msg(m_id)
        
        # if got msg with self as source, ignore msg
        if(source_id == receiver_id):
            print(f"[Node {receiver_id}:{seq_id}] received a msg claim that was sent by node it self", file=self.out_dolev)
            return
       
        # MD5 ignore msg if node already delivered the msg
        if(self.MD5 and self.delivered[m_id]):
            print(f"[Node {receiver_id}:{seq_id}] received a msg of a delivered msg, no futher processing", file=self.out_dolev)
            return
        
        # MD4 ignore msg with path that contain delivered neighbor
        if(self.MD4 and self.delivered_neighbors[m_id] & received_path_bitmask != 0):
            print(f"[Node {receiver_id}:{seq_id}] received path contain neighbors that already delivered, no futher processing", file=self.out_dolev)
            return
        
        # MD3 remove path from pathset that contain the sender how sent a empty path
        if(self.MD3 and received_path_bitmask == 0):
            sender_bitmask = 0b1 << sender_id
            self.delivered_neighbors[m_id] = self.delivered_neighbors[m_id] | sender_bitmask
            filtered_path = {path for path in self.paths[m_id] if (self.path_to_bitmask(path) & sender_bitmask == 0)}
            self.paths[m_id] = filtered_path
            print(f"[Node {receiver_id}:{seq_id}] received msg with empty path, current delivered neighbors: {self.formatPath(self.delivered_neighbors[m_id])}", file=self.out_dolev)
        
        try:
            # calculation of the path(received path + sender)
            updated_path = received_path
            if sender_id != source_id:
                updated_path.append(sender_id)
                
            # add path to pathset
            self.paths[m_id].add(tuple(updated_path))
                   
            # MD1 deliver the msg if source = sender
            if not self.is_byzantine and self.MD1 and (sender_id == source_id and not self.delivered[m_id]):
                await self.dolev_deliver(payload)
                print(f"[Node {receiver_id}:{seq_id}] sender = source, Delivered Message: [{m_id}]:{m}", file=self.out_dolev)
                self.logger.log_delivery(m_id, "Neighbor of Source")
                self.delivered[m_id] = True
                
            # deliver the msg if f+1 disjoint path found
            
            #criteria = self.criteria(self.paths[m_id])
            #self.has_exact_f_plus_one_vertex_disjoint_paths(self.paths[m_id],source_id, receiver_id)
            if not self.is_byzantine and not self.delivered[m_id]:
                criteria, disjoint_path = self.has_exact_f_plus_one_vertex_disjoint_paths(self.paths[m_id],source_id, receiver_id)
                if criteria:
                    # Delivered
                    await self.dolev_deliver(payload)
                    print(f"[Node {receiver_id}:{seq_id}] meet f+1 disjoint criteria, Delivered Message: [{m_id}]:{m}", file=self.out_dolev)
                    print(disjoint_path, file=self.out_dolev)
                    self.logger.log_delivery(m_id, "F+1 disjoint found", disjoint_path)
                    self.delivered[m_id] = True

            
            # MD2 if msg delivered, send empty path to neighbors and discard the pathset it was saving
            if self.MD2 and self.delivered[m_id]:
                updated_path = []
                self.paths[m_id] = set()
            
            # do not send to neighbors that is in the received_path or is source or sender
            no_sending_node = received_path_bitmask | (0b1 << source_id) 
            if not (len(received_path) != 0 and len(updated_path) == 0): # still send if receive not empty path and in sending empty path
                no_sending_node |= (0b1 << sender_id)
            # MD3 do not send msg to neighbors that already delivered the msg
            if(self.MD3): 
                no_sending_node |= self.delivered_neighbors[m_id]

            for neighbor in self.get_peers():
                neighbor_id = self.node_id_from_peer(peer=neighbor)
                neighbor_bitmask = 0b1 << neighbor_id
                if 0 != (neighbor_bitmask & no_sending_node):
                    continue
                # MD2 if msg delivered, stop discard msg with not empty path to neighbors
                if(self.delivered[m_id] and len(updated_path) != 0):
                    print(f"[Node {receiver_id}:{seq_id}] msg {m_id} delivered, stop sending path {updated_path} to neighbors", file=self.out_dolev)
                    break
                output = f"[Node {receiver_id}:{seq_id}] Send msg {m_id} with path {updated_path} to {neighbor_id}"
                await self.send_with_delay(neighbor, output, DolevMessage(source_id, receiver_id, m, m_id, updated_path))
                self.logger.sent_msg(m_id)

        except Exception as e:
            print(f"[Node {receiver_id}:{seq_id}] Error in al_deliver: {e}", file=self.out_dolev)
            raise e

        self.logger.write_log_to_output()
        
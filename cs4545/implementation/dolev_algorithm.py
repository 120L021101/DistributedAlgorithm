from typing import List
from cs4545.system.da_types import *


@dataclass(msg_id=4)  # The value 1 identifies this message and must be unique per community.
class DolevMessage:
    source_id : int
    sender_id : int
    message : str
    message_id : str
    path : int

class DolevAlgorithm(DistributedAlgorithm):
    """_summary_
    Assignment 1, reliable communication, dolev algorithm
    Args:
        DistributedAlgorithm (_type_): _description_
    """
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

        # 方便测试，针对latency和msg复杂度
        self.interface_recv_msg_cnt = 0
        self.interface_sent_msg_cnt = 0
        self.protocol_delivered_msg_cnt = 0

        self.add_message_handler(DolevMessage, self.al_deliver)

    async def on_start(self):
        # Make sure to call this one last in this function
        self.start_time = time.time()
        await super().on_start()
    
    # upon event ⟨Dolev, Broadcast | m⟩
    # only starter does "Broadcast"
    async def on_start_as_starter(self, message=None):
        
        # 如果是malicious broadcaster，只广播f个邻居，否则有多少广播多少
        broadcast_neighbor_upper_bound = None
        if self.is_byzantine:
            broadcast_neighbor_upper_bound = self.f
        else:
            broadcast_neighbor_upper_bound = len(self.get_peers())

        self.start_time = time.time()
        seq_id = self.seq_id
        seq_id += 1 
        print(f"[Node {self.node_id}] is starting the Delov algorithm")
        if message is None:
            message = f"Hello From {self.node_id}"
        for i in range(self.broadcast_num):
            message_id = f"{self.node_id}:{self.sent_msg_cnt}"
            self.sent_msg_cnt += 1
            for n_id, peer in enumerate(self.get_peers()):
                if n_id == broadcast_neighbor_upper_bound:
                    break
                # broadcast to all neighbors  
                peer_id = self.node_id_from_peer(peer=peer)
                print(f"[Node {self.node_id}] Broadcast to {peer_id}")  
                await self.send_with_delay(peer, "", DolevMessage(self.node_id,self.node_id, message, message_id, 0))
                self.logger.sent_msg(message_id)

            self.delivered[message_id] = True
            self.protocol_delivered_msg_cnt += 1
            # Delivered!
            print(f"[Node {self.node_id}] Delivered Message: [{message_id}]:{message}")
            self.logger.log_delivery(message_id, "As Source")
            self.logger.write_log_to_output()
    
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
        print(f"[Node {self.node_id}] check disjoint: {self.formatPaths(bit_paths)}")
        if(len(bit_paths) < self.f +1):
            return False
        bit_paths = list(bit_paths)
        return self.__dfs_max_disjoint(0, [0 for _ in range(len(bit_paths))],
                                       bit_paths, 0) >= self.f + 1
    
    def ignore_msg(self, payload):
        # byzantine: ignore msg, do nothing
        print(f"[Node {self.node_id}] ignores {payload.message_id} : {payload.message}")
        return False 
    
    def modify_msg_id(self, payload):
        # byzantine: increase source id by 10
        pj = payload.source_id
        payload.source_id += 10
        print(f"[Node {self.node_id}] modify {payload.message_id}'s source_id from {pj} to {payload.source_id}")
        payload.message_id = f"{payload.source_id}" + ":" + payload.message_id.split(":")[1]
        print(f"[Node {payload.source_id}] modify message id to {payload.message_id}")
        return True

    # upon event ⟨al, Deliver | pj , [m, path]⟩
    @message_wrapper(DolevMessage)
    async def al_deliver(self, neighbor: Peer, payload: DolevMessage) -> None:
        receiver_id = self.node_id
        sender_id, m, m_id, received_path, source_id = payload.sender_id, payload.message, payload.message_id, payload.path, payload.source_id
        seq_id = self.seq_id
        self.seq_id += 1
        
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
            if_continue = behaviour(*args)
            if not if_continue:
                return
            
        # initialize variables if needed
        if m_id not in self.paths:
            self.paths[m_id] = set()
        if m_id not in self.delivered:
            self.delivered[m_id] = False
            self.log_reason[m_id] = ""
            self.delivered_neighbors[m_id] = 0
        print(f"[Node {receiver_id}:{seq_id}] Got a message {m} source of {source_id} with sender {sender_id}.\t" + 
                f"msg_id: {m_id}, msg path: {self.formatPath(received_path)} " +
                                 f"and local paths: {self.formatPaths(self.paths[m_id])}")
        self.logger.recv_msg(m_id)
       
        # MD5 ignore msg if node already delivered the msg
        if(self.MD5 and self.delivered[m_id]):
            print(f"[Node {receiver_id}:{seq_id}] received a msg of a delivered msg, no futher processing")
            return
        
        # MD4 ignore msg with path that contain delivered neighbor
        if(self.MD4 and self.delivered_neighbors[m_id] & received_path != 0):
            print(f"[Node {receiver_id}:{seq_id}] received path contain neighbors that already delivered, no futher processing")
            return
        
        # MD3 remove path from pathset that contain the sender how sent a empty path
        if(self.MD3 and received_path == 0):
            sender_bitmask = 0b1 << sender_id
            self.delivered_neighbors[m_id] = self.delivered_neighbors[m_id] | sender_bitmask
            filtered_path = {path for path in self.paths[m_id] if (path & sender_bitmask == 0)}
            self.paths[m_id] = filtered_path
            print(f"[Node {receiver_id}:{seq_id}] received msg with empty path, current delivered neighbors: {self.formatPath(self.delivered_neighbors[m_id])}")
        
        try:
            # calculation of the path(received path + sender)
            updated_path = received_path
            if sender_id != source_id:
                updated_path = received_path | (0b1 << sender_id)
                
            # add path to pathset
            self.paths[m_id].add(updated_path)
                   
            # MD1 deliver the msg if source = sender
            if not self.is_byzantine and self.MD1 and (sender_id == source_id and not self.delivered[m_id]):
                print(f"[Node {receiver_id}:{seq_id}] sender = source, Delivered Message: [{m_id}]:{m}")
                self.logger.log_delivery(m_id, "Neighbor of Source")
                self.delivered[m_id] = True
                
            # deliver the msg if f+1 disjoint path found
            
            if not self.is_byzantine and not self.delivered[m_id] and self.criteria(self.paths[m_id]):
                # Delivered
                print(f"[Node {receiver_id}:{seq_id}] meet f+1 disjoint criteria, Delivered Message: [{m_id}]:{m}")
                self.logger.log_delivery(m_id, "F+1 disjoint found", None)
                self.delivered[m_id] = True

            
            # MD2 if msg delivered, send empty path to neighbors and discard the pathset it was saving
            if self.MD2 and self.delivered[m_id]:
                updated_path = 0
                self.paths[m_id] = set()
            
            # do not send to neighbors that is in the received_path or is source or sender
            no_sending_node = received_path | (0b1 << source_id) | (0b1 << sender_id)
            # MD3 do not send msg to neighbors that already delivered the msg
            if(self.MD3): 
                no_sending_node = no_sending_node | self.delivered_neighbors[m_id]

            for neighbor in self.get_peers():
                neighbor_id = self.node_id_from_peer(peer=neighbor)
                neighbor_bitmask = 0b1 << neighbor_id
                if 0 != (neighbor_bitmask & no_sending_node):
                    continue
                # MD2 if msg delivered, stop discard msg with not empty path to neighbors
                if(self.delivered[m_id] and updated_path != 0):
                    print(f"[Node {receiver_id}:{seq_id}] msg {m_id} delivered, stop sending path {self.formatPath(updated_path)} to neighbors")
                    break
                output = f"[Node {receiver_id}:{seq_id}] Send msg {m_id} with path {self.formatPath(updated_path)} to {neighbor_id}"
                await self.send_with_delay(neighbor, output, DolevMessage(source_id, receiver_id, m, m_id, updated_path))
                self.logger.sent_msg(m_id)

        except Exception as e:
            print(f"[Node {receiver_id}:{seq_id}] Error in al_deliver: {e}")
            raise e

        self.logger.write_log_to_output()
        
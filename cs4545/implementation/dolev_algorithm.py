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
        self.start_time = time.time()
        seq_id = self.seq_id
        seq_id += 1 
        print(f"Node {self.node_id} is starting the Delov algorithm")
        if message is None:
            message = f"Hello From {self.node_id}"
        for i in range(self.broadcast_num):
            message_id = f"{self.node_id}:{self.sent_msg_cnt}"
            self.sent_msg_cnt += 1
            for peer in self.get_peers():
                # broadcast to all neighbors  
                peer_id = self.node_id_from_peer(peer=peer)
                print(f"[Node {self.node_id}] Send to {peer_id}")  
                await self.send_with_delay(peer, DolevMessage(self.node_id, self.node_id, message, message_id, 0))
                self.interface_sent_msg_cnt += 1

            self.delivered[message_id] = True
            self.protocol_delivered_msg_cnt += 1
            # Delivered!
            print(f"[Node {self.node_id}] Delivered Message: [{message_id}]:{message}")
    
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
        # byzantine: increase origin id by 10
        pj = payload.ori_id
        payload.ori_id += 10
        print(f"[Node {self.node_id}] modify {payload.message_id}'s m_id from {pj} to {payload.ori_id}")
        return True

    # upon event ⟨al, Deliver | pj , [m, path]⟩
    @message_wrapper(DolevMessage)
    async def al_deliver(self, neighbor: Peer, payload: DolevMessage) -> None:
        # if is byzantine node, act based on byzantine behaviour
        if self.is_byzantine:
            (behaviour, args) = {
                # 忽略消息
                "IGNORE_MSG": (self.ignore_msg, (payload, )),
                # 修改消息的origin id，消息源
                "MODIFY_MSG_ID": (self.modify_msg_id, (payload,)),
            }.get(self.byzantine_behaviour)

            # e.g. if ignore, return, if modify, continue
            if_continue = behaviour(*args)
            if not if_continue:
                return
            
        receiver_id = self.node_id
        sender_id, m, m_id, received_path, source_id = payload.sender_id, payload.message, payload.message_id, payload.path, payload.source_id
        seq_id = self.seq_id
        self.seq_id += 1

        # initialize variables if needed
        if m_id not in self.paths:
            self.paths[m_id] = set()
        if m_id not in self.delivered:
            self.delivered[m_id] = False
            self.delivered_neighbors[m_id] = 0
        print(f"[Node {receiver_id}:{seq_id}] Got a message {m} source of {source_id} with sender {sender_id}.\t" + 
                f"msg_id: {m_id}, msg path: {self.formatPath(received_path)} " +
                                 f"and local paths: {self.formatPaths(self.paths[m_id])}")
        self.interface_recv_msg_cnt += 1
       
        # MD5
        if(self.MD5 and self.delivered[m_id]):
            print(f"[Node {receiver_id}:{seq_id}] received a msg of a delivered msg, no futher processing")
            return
        
        # MD3
        if(self.MD3 and received_path == 0):
            self.delivered_neighbors[m_id] = self.delivered_neighbors[m_id] | (0x1 << sender_id)
            print(f"[Node {receiver_id}:{seq_id}] received msg with empty path, current delivered neighbors: {self.formatPath(self.delivered_neighbors[m_id])}")
            
        # MD4
        if(self.MD4 and self.delivered_neighbors[m_id] & received_path != 0):
            print(f"[Node {receiver_id}:{seq_id}] received path contain neighbors that already delivered, no futher processing")
            return
        
        try:
            updated_path = received_path
            if sender_id != source_id:
                updated_path = received_path | (0x1 << sender_id)
                
            self.paths[m_id].add(updated_path)     
                   
            # MD1
            if not self.is_byzantine and self.MD1 and (sender_id == source_id and not self.delivered[m_id]):
                print(f"[Node {receiver_id}:{seq_id}] sender = source, Delivered Message: [{m_id}]:{m}")
                self.delivered[m_id] = True
                self.protocol_delivered_msg_cnt += 1
                
            # byzantine node deliver doesn't count
            if not self.is_byzantine and not self.delivered[m_id] and self.criteria(self.paths[m_id]):
                # Delivered
                print(f"[Node {receiver_id}:{seq_id}] meet f+1 disjoint criteria, Delivered Message: [{m_id}]:{m}")
                self.delivered[m_id] = True
                self.protocol_delivered_msg_cnt += 1

            
            # MD2 task1, send empty path, so updated_path should be empty
            # MD2 task2, broadcast to all neighbors, so self.paths[m_id] should be empty
            if self.MD2 and self.delivered[m_id]:
                updated_path = 0
                self.paths[m_id] = set()
            
            
            # do not send to occurred neighbors
            no_sending_node = received_path | (0x1 << source_id) | (0x1 << sender_id)
            # MD3
            if(self.MD3): 
                no_sending_node = no_sending_node | self.delivered_neighbors[m_id]
            for neighbor in self.get_peers():
                neighbor_id = self.node_id_from_peer(peer=neighbor)
                neighbor_bitmask = 0x1 << neighbor_id
                if 0 != (neighbor_bitmask & no_sending_node):
                    continue
                print(f"[Node {receiver_id}:{seq_id}] Send msg {m_id} with path {self.formatPath(updated_path)} to {neighbor_id}")  
                await self.send_with_delay(neighbor, DolevMessage(source_id, receiver_id, m, m_id, updated_path))
                self.interface_sent_msg_cnt += 1

        except Exception as e:
            print(f"[Node {receiver_id}:{seq_id}] Error in al_deliver: {e}")
            raise e

        # log output to "output/node{node_id}.log"
        self.log()
        
    def log(self):
        self.log_file.write(f"({time.time()-self.start_time}, {self.interface_recv_msg_cnt}, {self.interface_sent_msg_cnt}, {self.protocol_delivered_msg_cnt})\n")
        self.log_file.flush()
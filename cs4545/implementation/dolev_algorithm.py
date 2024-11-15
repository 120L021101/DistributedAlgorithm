from typing import List
from cs4545.system.da_types import *


@dataclass(msg_id=4)  # The value 1 identifies this message and must be unique per community.
class DolevMessage:
    ori_id : int
    src_id : int
    message : str
    message_id : str
    path : int

class DolevAlgorithm(DistributedAlgorithm):
    """_summary_
    Assignment 1, reliable communication, dolev algorithm
    Args:
        DistributedAlgorithm (_type_): _description_
    """

    # upon event ⟨Dolev, Init⟩ 
    def __init__(self, settings: CommunitySettings) -> None:
        # node_id, neighbors etc, all in this "settings"
        super().__init__(settings)

        self.delivered = dict()
        self.paths = dict()
        self.sent_msg_cnt = 0

        self.add_message_handler(DolevMessage, self.al_deliver)

    async def on_start(self):
        # Make sure to call this one last in this function
        await super().on_start()
    
    # upon event ⟨Dolev, Broadcast | m⟩
    # only starter does "Broadcast"
    async def on_start_as_starter(self, message=None):
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
            self.delivered[message_id] = True
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
        max_val = self.__dfs_max_disjoint(idx + 1, cur_picks, bit_paths, max_val)
        cur_picks[idx] = 1
        return self.__dfs_max_disjoint(idx + 1, cur_picks, bit_paths, max_val)
        
    
    def criteria(self, bit_paths: set) -> bool:
        bit_paths = list(bit_paths)
        return self.__dfs_max_disjoint(0, [0 for _ in range(len(bit_paths))],
                                       bit_paths, 0) == self.f + 1
    
    # upon event ⟨al, Deliver | pj , [m, path]⟩
    @message_wrapper(DolevMessage)
    async def al_deliver(self, peer: Peer, payload: DolevMessage) -> None:
        pi = self.node_id
        pj, m, m_id, path, ori_id = payload.src_id, payload.message, payload.message_id, payload.path, payload.ori_id
        if m_id not in self.paths:
            self.paths[m_id] = set()
        if m_id not in self.delivered:
            self.delivered[m_id] = False
        print(f"[Node {pi}] Got a message {m} origin of {ori_id} with sender {pj}.\t \
                msg_id: {m_id}, msg path: {path} and pi paths: {self.paths[m_id]}")
        try:
            path_to_send = path
            if pj != ori_id:
                path_to_send = path | (0x1 << pj)
                
            self.paths[m_id].add(path_to_send)
            
            #for p in path + [pj]:
            #    self.paths[m_id].add(p)
                # check event "Dolev Deliver" condition
                # upon event (pi is connected to the source through f + 1 node-disjoint paths contained in paths) and delivered = False
            if not self.delivered[m_id] and self.criteria(self.paths[m_id]):
                # Delivered
                print(f"[Node {pi}] Delivered Message: [{m_id}]:{m}")
                self.delivered[m_id] = True
            print(f"Paths after for loop: {self.paths[m_id]}")

            path_merged = path | (0x1 << ori_id) | (0x1 << pj)
            pi_bit = 0x1 << pi
            for peer in self.get_peers():
                pk = self.node_id_from_peer(peer=peer)
                if 0 != (pi_bit & path_merged):
                    continue
                print(f"[Node {self.node_id}] Send to {pk}")  
                await self.send_with_delay(peer, DolevMessage(ori_id, pi, m, m_id, path_to_send))

        except Exception as e:
            print(f"[Node {self.node_id}] Error in al_deliver: {e}")
            raise e
    
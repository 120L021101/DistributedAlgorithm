from typing import List
from cs4545.system.da_types import *


@dataclass(msg_id=4)  # The value 1 identifies this message and must be unique per community.
class DolevMessage:
    ori_id : int
    src_id : int
    message : str
    message_id : str
    path : List[int]

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
        return await self.broadcast_msg(message=message)

    async def broadcast_msg(self, message=None):
        print(f"Node {self.node_id} is starting the Delov algorithm")
        if message is None:
            message = f"Hello From {self.node_id}"
        for i in range(self.broadcast_num):
            message_id = f"{self.node_id}:{self.sent_msg_cnt}"
            self.sent_msg_cnt += 1
            for peer in self.get_peers():
                # broadcast to all neighbors  
                peer_id = self.node_id_from_peer(peer=peer)
                # print(f"[Node {self.node_id}] Send to {peer_id}")  
                await self.send_with_delay(peer, DolevMessage(self.node_id, message, message_id, []))
            self.delivered[message_id] = True
            # Delivered!
            print(f"[Node {self.node_id}] Delivered Message: [{message_id}]:{message}")
    
    # upon event ⟨al, Deliver | pj , [m, path]⟩
    @message_wrapper(DolevMessage)
    async def al_deliver(self, peer: Peer, payload: DolevMessage) -> None:
        pi = self.node_id
        pj, m, m_id, path = payload.node_id, payload.message, payload.message_id, payload.path
        if m_id not in self.paths:
            self.paths[m_id] = set()
        if m_id not in self.delivered:
            self.delivered[m_id] = False
        # print(f"[Node {pi}] Got a message {m} from node: {pj}.\t \
        #         msg_id: {m_id}, msg path: {path} and pi paths: {self.paths[m_id]}")
        try:
            for p in path + [pj]:
                self.paths[m_id].add(p)
                # check event "Dolev Deliver" condition
                # upon event (pi is connected to the source through f + 1 node-disjoint paths contained in paths) and delivered = False
                if not self.delivered[m_id] and len(self.paths[m_id]) == self.f + 1:
                    # Delivered
                    print(f"[Node {pi}] Delivered Message: [{m_id}]:{m}")
                    self.delivered[m_id] = True                

            for peer in self.get_peers():
                pk = self.node_id_from_peer(peer=peer)
                if pk in path or pk == pj:
                    continue
                # print(f"[Node {self.node_id}] Send to {pk}")  
                await self.send_with_delay(peer, DolevMessage(pk, m, m_id, path + [pj]))

        except Exception as e:
            print(f"[Node {self.node_id}] Error in al_deliver: {e}")
            raise e
    
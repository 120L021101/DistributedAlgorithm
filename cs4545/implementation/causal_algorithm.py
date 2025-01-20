from typing import List

import json

from cs4545.system.da_types import *
from cs4545.implementation.bracha_algorithm import BrachaAlgorithm
from cs4545.system.causal_logger import CausalLogger

import json

class RCOMsg:
    
    @staticmethod
    def decode(message):
        json_msg = json.loads(message)
        return RCOMsg(VC=json_msg['VC'], msg=json_msg['m'])

    def __init__(self, VC, msg):
        self.VC = VC
        self.msg = msg

    def encode(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return json.dumps({
            "VC" : self.VC,
            "m" : self.msg
        })
    
    def __eq__(self, other):
        if not isinstance(other, RCOMsg):
            return False
        return f"{self.VC}" == f"{other.VC}" and self.msg == other.msg

    def __hash__(self):
        # Use tuple of (VC, msg) to create a hash
        return hash((f"{self.VC}", self.msg))
    
    def __repr__(self):
        return self.__str__()

class CausalAlgorithm(BrachaAlgorithm):
    """_summary_
    Assignment 3, reliable causal-order broadcast
    Args:
        DistributedAlgorithm (_type_): _description_
    """

    """
    Note: rank[pi] = pi
    """

    # upon event <init> 
    def __init__(self, settings: CommunitySettings) -> None:
        super().__init__(settings)

        '''
        self.N 这个变量在init的时候还没初始化, 延迟一下Init到self.real_init
        '''
    
    # upon event <init> 
    def real_init(self):
        stat_file: str = "output/node.yml"
        stat_file = Path(stat_file)
        stat_file = stat_file.parent / f"causal-{stat_file.stem}-{self.node_id}{stat_file.suffix}"
        self.causal_logger = CausalLogger(stat_file,self.node_id)
        
        # forall pi do VC[rank pi] = 0
        self.VC = [0] * self.N # VC: vector clock
        self.pending = set()

    def is_next(self, msg_sender, msg_VC):
        VC_to_compare = self.VC.copy()
        VC_to_compare[msg_sender] = VC_to_compare[msg_sender] + 1
        return all(VC_to_compare[pj] >= msg_VC[pj] for pj in range(len(VC_to_compare)))
        
    def rco_deliver(self, rco_data):
        print(f"[RCO Node {self.node_id}]: deliver {rco_data.encode()}")
        self.causal_logger.log_delivery(rco_data.msg)
        self.causal_logger.write_log_to_output()
        
    async def on_start(self):
         
        self.real_init()
        await super().on_start()
    
    # upon event <rcoBroadcast | m>
    async def on_start_as_starter(self):

        self.real_init()
        
        msg_to_send = []
        print(f"[Node {self.node_id}] is starting the Causal algorithm")
        for i in range(self.broadcast_num):
            msg = f"[{self.node_id}:{i}] Message from {self.node_id} id {i}"
            msg_to_send.append(self.rco_broadcast(msg))
            
        random.shuffle(msg_to_send)
        for m in msg_to_send:
            await self.bracha_broadcast(data = m)

    def rco_broadcast(self, m):
        rco_data = RCOMsg(self.VC, m)

        # trigger <rcoDeliver | self, m>>
        self.rco_deliver(rco_data)
        self.VC[self.node_id] = self.VC[self.node_id] + 1
            
        return rco_data.encode()

    # upon event <rbDeliver | pi, [VCm, m]>
    async def bracha_deliver(self, pi, message):
        # pi 是bracha的origin_id
        rco_data = RCOMsg.decode(message)

        if pi == self.node_id:
            return
        
        print(f"[RCO Node {self.node_id}]: receive {rco_data.encode()}")
        self.causal_logger.log_receive_msg(rco_data.msg)
        
        if not self.is_next(pi,rco_data.VC):
            # pending := pending ∪ (pi, rco_data)
            self.pending.add((pi, rco_data))
        
        else:
            self.rco_deliver(rco_data)
            self.VC[pi] = self.VC[pi] + 1
        
            while True:
                again = False
                deliverable = []
                for (Sx, rco_data_x) in list(self.pending):
                    print(f"Msg from {Sx} with VC {rco_data_x}, local VC is {self.VC}")
                    if self.is_next(Sx, rco_data_x.VC):
                        deliverable.append((Sx, rco_data_x))
                        break

                # Deliver after collecting
                for (Sx, rco_data_x) in deliverable:
                    self.pending.remove((Sx, rco_data_x))
                    self.rco_deliver(rco_data_x)
                    self.VC[Sx] = self.VC[Sx] + 1
                    again = True

                if not again:
                    break

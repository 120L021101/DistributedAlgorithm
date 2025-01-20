from typing import List

import os, sys
from enum import Enum
import json
import math

from cs4545.system.da_types import *
from cs4545.implementation.dolev_algorithm import DolevMessage, DolevAlgorithm
from cs4545.system.bracha_logger import BrachaLogger

class Phase(Enum):
    SEND = 0
    ECHO = 1
    READY = 2

class PhaseEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Phase):
            return obj.name
        return super().default(obj)

class BrachaAlgorithm(DolevAlgorithm):
    """_summary_
    Assignment 2, reliable broadcast, bracha algorithm
    Args:
        DistributedAlgorithm (_type_): _description_
    """

    """
    BRB消息格式
    brb_msg_id, 与Dolev无关, 本协议的标号
    phase, 三轮转发阶段标识, SEND, ECHO, READY
    sender_id, 消息发送者的标号
    data, 上层协议信息
    """
    
    async def started(self, *args, **kwargs):
        await super().started(*args, **kwargs)
        stat_file: str = "output/node.yml"
        stat_file = Path(stat_file)
        stat_file = stat_file.parent / f"bracha-{stat_file.stem}-{self.node_id}{stat_file.suffix}"
        self.bracha_logger = BrachaLogger(stat_file,self.node_id,is_byzantine=self.is_brb_byzantine,byzantine_behaviour=self.byzantine_behaviour)
        

    # log for brb broadcast, no dolev output
    def broadcast_log(self, message):
        if isinstance(message, str):
            decode_msg = json.loads(message)
            decode_msg['phase'] = Phase[decode_msg['phase']]
        else:
            decode_msg = message
        print(f"[BRB Node {self.node_id}] broadcast brb_id {decode_msg['brb_msg_id']} phase {decode_msg['phase']}", file=self.out_bracha)

    def fakeMsg(self, phase: Phase):
        return {"phase": phase,
                    "brb_msg_id": f"BRB_{100}:{0}",
                    "sender_id": self.node_id,
                    "origin_id": self.node_id,
                    "data": "hello world"}

    # new message with "SEND" phase
    def newMsg(self, data):
        brb_seq_id = self.brb_seq_id
        self.brb_seq_id += 1
        return json.dumps({"phase": Phase.SEND,
                    "brb_msg_id": f"BRB_{self.node_id}:{brb_seq_id}",
                    "sender_id": self.node_id,
                    "origin_id": self.node_id,
                    "data": data}, cls=PhaseEncoder)

    # parse brb message from dolev payload
    def parseMsg(self, payload):
        message = json.loads(payload.message)
        message['phase'] = Phase[message['phase']]
        return message
    
    # from send to echo, or from echo to ready
    def msgToNextPhase(self, message):
        if isinstance(message, str):
            message = json.loads(message)

        message['phase'] = {
            Phase.SEND : Phase.ECHO,
            Phase.ECHO : Phase.READY
        }.get(message['phase'])
        
        return json.dumps(message, cls=PhaseEncoder)
    
    # change sender id to myself
    def itsMeSending(self, message):
        if isinstance(message, str):
            message = json.loads(message)
        message['sender_id'] = self.node_id
        return json.dumps(message, cls=PhaseEncoder)

    # upon event <Bracha, Init> 
    def __init__(self, settings: CommunitySettings) -> None:
        super().__init__(settings)

        self.brb_sentEcho = {}
        self.brb_sentReady = {}
        self.brb_delivered = {}

        self.brb_echos = {}
        self.brb_readys = {}

        self.brb_seq_id = 0
        
        self.sentFakeEcho = False
        self.sentFakeReady = False

        self.out_bracha = open(os.devnull, mode='w', encoding='utf-8')

    # upon even <Bracha, Broadcast | m>
    async def on_start_as_starter(self, data=None):
        print(f"[Node {self.node_id}] is starting the Bracha algorithm", file=self.out_bracha)
        await self.bracha_broadcast(data=data)

    async def bracha_broadcast(self, data=None):
        if data is None:
            message = self.newMsg(data="hello world")
        else:
            message = self.newMsg(data=data)

        self.broadcast_log(message)
        
        message = self.itsMeSending(message)

        self.bracha_logger.sent_msg(message)
        self.bracha_logger.set_is_broadcaster()
        await self.dolev_broadcast(message=message)

    # dolev protocol deliver some msg to this brb protocol
    async def dolev_deliver(self, payload):
        message = self.parseMsg(payload)
        self.bracha_logger.recv_msg(message)
        print(f"[BRB Node {self.node_id}] receive Phase {message['phase']}, BRB id is {message['brb_msg_id']}", file=self.out_bracha)
        
        # Malicius node
        if self.is_brb_byzantine:
            # print("BYZANTINE")
            if self.byzantine_behaviour == "IGNORE_MSG":
                print(f"[BRB Node {self.node_id}] ignore msg {message['brb_msg_id']}", file=self.out_bracha)
            elif self.byzantine_behaviour == "COLLUDE":
                if message['phase'] == Phase.SEND and not self.sentFakeEcho:
                    fake_msg = self.fakeMsg(Phase.SEND)
                    print(f"[BRB Node {self.node_id}] create fake ECHO msg {fake_msg['brb_msg_id']}", file=self.out_bracha)
                    self.sentFakeEcho = True
                    await self.on_deliver_and_send(fake_msg)
                elif message['phase'] == Phase.ECHO and not self.sentFakeReady:
                    fake_msg = self.fakeMsg(Phase.ECHO)
                    print(f"[BRB Node {self.node_id}] create fake READY msg {fake_msg['brb_msg_id']}", file=self.out_bracha)
                    self.sentFakeReady = True
                    await self.on_deliver_and_echo(fake_msg)
            self.bracha_logger.write_log_to_output()
            return
        

        if message['phase'] == Phase.SEND and (message['brb_msg_id'] not in self.brb_sentEcho):
            await self.on_deliver_and_send(message)

        elif message['phase'] == Phase.ECHO:
            await self.on_deliver_and_echo(message)
        
        elif message['phase'] == Phase.READY:
            await self.on_deliver_and_ready(message)

    # upon event <al, Deliver | p, [SEND, m]> and not sentEcho
    async def on_deliver_and_send(self, message):
        # message phase is SEND
        msg_to_send = self.msgToNextPhase(message)
        self.brb_sentEcho[message['brb_msg_id']] = True

        self.broadcast_log(msg_to_send)

        msg_to_send = self.itsMeSending(msg_to_send)
        self.bracha_logger.sent_msg(msg_to_send)
        await self.dolev_broadcast(msg_to_send)
        
    # upon event <al, Deliver | p, [ECHO, m]> do
    async def on_deliver_and_echo(self, message):
        # message phase is ECHO
        self.brb_echos.setdefault(message['brb_msg_id'], set()) \
                      .add(message['sender_id'])
        
        await self.check_echo_size_and_not_send_ready(message)
        
    # upon event <al, Deliver | p, [READY, m]> do
    async def on_deliver_and_ready(self, message):
        # message phase is READY
        self.brb_readys.setdefault(message['brb_msg_id'], set()) \
                      .add(message['sender_id'])
        
        await self.check_readys_size_and_not_send_ready(message)
        await self.check_if_can_deliver(message)
    
    # upon event echos.size() ≥ ⌈ N +f +1/ 2 ⌉ and not sentReady do
    async def check_echo_size_and_not_send_ready(self, message):
        # message phase is ECHO
        print(f"[BRB Node {self.node_id}] brb echos length is {len(self.brb_echos[message['brb_msg_id']])}", file=self.out_bracha)
        if not len(self.brb_echos[message['brb_msg_id']]) >= math.ceil((self.N + self.brb_f + 1) / 2):
            return
        if message['brb_msg_id'] in self.brb_sentReady:
            return
        
        msg_ready = self.msgToNextPhase(message)
        self.brb_sentReady[message['brb_msg_id']] = True
        
        self.broadcast_log(msg_ready)

        msg_ready = self.itsMeSending(msg_ready)
        self.bracha_logger.sent_msg(msg_ready)
        await self.dolev_broadcast(msg_ready)

    # upon event readys.size() ≥ f +1 and not sentReady
    async def check_readys_size_and_not_send_ready(self, message):
        # message phase is READY
        if not len(self.brb_readys[message['brb_msg_id']]) >= self.brb_f + 1:
            return
        if message['brb_msg_id'] in self.brb_sentReady:
            return
        
        self.brb_sentReady[message['brb_msg_id']] = True
        
        self.broadcast_log(message)

        message = self.itsMeSending(message)
        self.bracha_logger.sent_msg(message) 
        await self.dolev_broadcast(message)

    # upon event readys.size() ≥ 2*f +1 and not sentReady
    async def check_if_can_deliver(self, message):
        # message phase is READY
        if not len(self.brb_readys[message['brb_msg_id']]) >= 2 * self.brb_f + 1:
            return
        if message['brb_msg_id'] in self.delivered:
            return
        
        self.delivered[message['brb_msg_id']] = True
        self.bracha_logger.log_delivery(message)
        self.bracha_logger.write_log_to_output()
        # Delivered!
        print(f"[BRB Node {self.node_id}] delivered {message['brb_msg_id']}", file=self.out_bracha)

        await self.bracha_deliver(message['origin_id'], message['data'])

    # bracha deliver to upper protocol: causal order
    async def bracha_deliver(self, pi, message):
        # print(message)
        return
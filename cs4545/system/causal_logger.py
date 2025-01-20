import json
from pathlib import Path
import time, yaml
import re

class CausalRecord:
    def __init__(self, sender) -> None:
        self.sender = sender
        self.receive_order = []
        self.deliver_order = []
        self.deliver_time = []
        
    def deliver(self, id, time):
        self.deliver_order.append(id)
        self.deliver_time.append(time)
            
    def receive(self, id):
        self.receive_order.append(id)
            
    def to_dict(self):
        return {
            "sender": self.sender,
            "receive_order": self.receive_order,
            "deliver_order": self.deliver_order,
            "deliver_time": self.deliver_time,
        }
            

class CausalLogger:
    def __init__(self, file_path, node_id) -> None:
        self.node_id = node_id
        self.is_broadcaster = False
        self.start_time = time.time()
        self.log = dict()
        self.file_path = file_path
        self.deliver_order = []
        
    def set_is_broadcaster(self):
        self.is_broadcaster = True
        
    def get_record(self, sender) -> CausalRecord:
        if sender not in self.log.keys():
            self.log[sender] = CausalRecord(sender)
        return self.log[sender]
        
    def get_sender_and_id(self, msg):
        match = re.search(r"\[(\d+):(\d+)\]", msg)
        if match:
            sender = match.group(1)
            seq_id = match.group(2)
            
        return sender, seq_id
            
    def log_delivery(self, msg):
        sender, id = self.get_sender_and_id(msg)
        self.get_record(sender).deliver(id,time.time()-self.start_time)
        self.deliver_order.append(f"{sender}:{id}")
        
    def log_receive_msg(self, msg):
        sender, id = self.get_sender_and_id(msg)
        self.get_record(sender).receive(id)
        
    def write_log_to_output(self):
        all_deliver_times = []
        for record in self.log.values():
            all_deliver_times.extend(record.deliver_time)

        if len(all_deliver_times) > 0:
            average_delivery_time = sum(all_deliver_times) / len(all_deliver_times)
        else:
            average_delivery_time = 0
        dic = {
            "node_id": self.node_id,
            "records": {msg_id: record.to_dict() for msg_id, record in self.log.items()},
            "total_delivered_msg": sum(len(record.deliver_order) for record in self.log.values()),
            "average_delivery_time": average_delivery_time,
            "deliver_order": self.deliver_order
        }
        p = Path(self.file_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(self.file_path, 'w') as yaml_file:
            yaml.dump(
                dic,
                yaml_file,
                default_flow_style=False
            )
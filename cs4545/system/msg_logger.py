from pathlib import Path
import time, yaml

class Record:
    def __init__(self, msg_id) -> None:
        self.msg_id = msg_id
        self.delivered = False
        self.recv_msg_count = 0
        self.send_msg_count = 0
        self.disjoint_path = None
        
    def recv_msg(self):
        self.recv_msg_count += 1
        
    def sent_msg(self):
        self.send_msg_count += 1
        
    def deliver(self, reason, time, disjoint_path=None):
        if(not self.delivered):
            self.deliver_reason = reason
            self.time_spent = time
            self.delivered = True
            self.disjoint_path = disjoint_path
            
    def to_dict(self):
        return {
            "msg_id": self.msg_id,
            "delivered": self.delivered,
            "recv_msg_count": self.recv_msg_count,
            "send_msg_count": self.send_msg_count,
            "deliver_reason": getattr(self, "deliver_reason", None),
            "time_spent": getattr(self, "time_spent", None),
            "disjoint_path": self.disjoint_path,
        }
            

class Logger:
    def __init__(self, file_path, node_id, byzantine_behaviour, is_byzantine=False) -> None:
        self.node_id = node_id
        self.is_byzantine = is_byzantine
        self.byzantine_behaviour = byzantine_behaviour
        self.start_time = time.time()
        self.log = dict()
        self.total_sent_msg_count = 0
        self.total_recv_msg_count = 0
        self.file_path = file_path
        
        
    def get_record(self, msg_id) -> Record:
        if msg_id not in self.log.keys():
            self.log[msg_id] = Record(msg_id)
        return self.log[msg_id]
        
    def sent_msg(self, msg_id):
        self.get_record(msg_id).sent_msg()
        self.total_sent_msg_count += 1
        
    def recv_msg(self, msg_id):
        self.get_record(msg_id).recv_msg()
        self.total_recv_msg_count += 1
    
    def log_delivery(self, message_id, deliver_reason, disjoint_path=None):
        self.get_record(message_id).deliver(deliver_reason, time.time()-self.start_time, disjoint_path)
        
    def write_log_to_output(self):
        dic = {
            "node_id": self.node_id,
            "total_sent_msg_count": self.total_sent_msg_count,
            "total_recv_msg_count": self.total_recv_msg_count,
            "records": {msg_id: record.to_dict() for msg_id, record in self.log.items()},
            "is_byzantine": self.is_byzantine,
            "byzantine_behaviour": self.byzantine_behaviour if self.is_byzantine else "-",
            "total_delivered_msg": sum(1 for record in self.log.values() if record.delivered)
        }
        p = Path(self.file_path)
        p.parent.mkdir(parents=True, exist_ok=True)
        with open(self.file_path, 'w') as yaml_file:
            yaml.dump(
                dic,
                yaml_file,
                default_flow_style=False
            )
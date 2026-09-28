from common import message_protocol
import uuid


class MessageHandler:
    def __init__(self):
        self.id = uuid.uuid4().hex
        self.client_messages_amount = 0
    
    def serialize_data_message(self, message):
        self.client_messages_amount += 1
        [fruit, amount] = message
        return message_protocol.internal.serialize([self.id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([self.id, self.client_messages_amount])

    def deserialize_result_message(self, message):
        client_id, fruit_top = message_protocol.internal.deserialize(message)
        if self.id != client_id:
            return None
        return fruit_top

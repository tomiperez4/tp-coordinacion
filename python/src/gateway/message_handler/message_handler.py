from common import message_protocol
import uuid

FRUIT_ITEM_OPCODE = message_protocol.internal.Opcode.FruitItem
EOF_OPCODE = message_protocol.internal.Opcode.EOF
FRUIT_TOP_OPCODE = message_protocol.internal.Opcode.FruitTop

class MessageHandler:
    def __init__(self):
        self.id = uuid.uuid4().hex
        self.client_messages_amount = 0
    
    def serialize_data_message(self, message):
        self.client_messages_amount += 1
        [fruit, amount] = message
        return message_protocol.internal.serialize([FRUIT_ITEM_OPCODE, self.id, fruit, amount])

    def serialize_eof_message(self, message):
        return message_protocol.internal.serialize([EOF_OPCODE, self.id, self.client_messages_amount])

    def deserialize_result_message(self, message):
        opcode, deserialized = message_protocol.internal.deserialize(message)
        client_id, fruit_top = deserialized
        if self.id != client_id or opcode != FRUIT_TOP_OPCODE:
            return None
        return fruit_top

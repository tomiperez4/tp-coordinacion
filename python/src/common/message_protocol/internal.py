import json
from enum import IntEnum

class Opcode(IntEnum):
    FruitItem = 0
    EOF = 1
    FruitTop = 2
    PartialTop = 3
    Control = 4
    Fruits = 5



def serialize(message):
    return json.dumps(message).encode("utf-8")


def deserialize(message):
    fields = json.loads(message.decode("utf-8"))
    return fields[0], fields[1:]

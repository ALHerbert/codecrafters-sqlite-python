from .varint_parser import parse_varint

def parse_record(stream, column_count):
    """
    Parses SQLite's "Record Format" as mentioned here: https://www.sqlite.org/fileformat.html#record_format
    """
    initial_position = stream.tell()
    _number_of_bytes_in_header = parse_varint(stream)
    body_start = initial_position + _number_of_bytes_in_header

    serial_types = [parse_varint(stream) for i in range(column_count)]
    stream.seek(body_start) # added this because for some reason there is empty space at the end of the header
    return [parse_column_value(stream, serial_type) for serial_type in serial_types]


def parse_column_value(stream, serial_type):
    if (serial_type >= 13) and (serial_type % 2 == 1):
        # Text encoding
        n_bytes = (serial_type - 13) // 2
        result = stream.read(n_bytes)
        return result

    elif serial_type == 0:
        return None
    elif serial_type == 1:
        # 8 bit twos-complement integer
        return int.from_bytes(stream.read(1), "big")
    elif serial_type == 2:
        return int.from_bytes(stream.read(2), "big")
    elif serial_type == 3:
        return int.from_bytes(stream.read(3), "big")
    elif serial_type ==4:
        return int.from_bytes(stream.read(4), "big")
    elif serial_type == 8:
        return int(0)
    elif serial_type == 9:
        return int(1)
    else:
        # There are more cases to handle, fill this in as you encounter them.
        raise Exception(f"Unhandled serial_type {serial_type}")

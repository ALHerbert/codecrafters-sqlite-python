import sys

from dataclasses import dataclass

import sqlparse 
from sqlparse.sql import IdentifierList, Function, Identifier

from .record_parser import parse_record
from .varint_parser import parse_varint


database_file_path = sys.argv[1]
command = sys.argv[2]


@dataclass(init=False)
class PageHeader:
    page_type: int
    first_free_block_start: int
    number_of_cells: int
    start_of_content_area: int
    fragmented_free_bytes: int

    @classmethod
    def parse_from(cls, database_file):
        """
        Parses a page header as mentioned here: https://www.sqlite.org/fileformat2.html#b_tree_pages
        """
        instance = cls()

        instance.page_type = int.from_bytes(database_file.read(1), "big")
        instance.first_free_block_start = int.from_bytes(database_file.read(2), "big")
        instance.number_of_cells = int.from_bytes(database_file.read(2), "big")
        instance.start_of_content_area = int.from_bytes(database_file.read(2), "big")
        instance.fragmented_free_bytes = int.from_bytes(database_file.read(1), "big")

        return instance

def get_page_size(database_path):
    with open(database_path, "rb") as database_file:
        database_file.seek(16)
        page_size = int.from_bytes(database_file.read(2), "big")
        return page_size

def generate_schema_rows(database_path):
    with open(database_path, "rb") as database_file:
        database_file.seek(100)  # Skip the header section
        page_header = PageHeader.parse_from(database_file)
        database_file.seek(100+8)  # Skip the database header & b-tree page header, get to the cell pointer array
        
        cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]
        sqlite_schema_rows = []

        # Each of these cells represents a row in the sqlite_schema table.
        for cell_pointer in cell_pointers:
            database_file.seek(cell_pointer)
            _number_of_bytes_in_payload = parse_varint(database_file)
            rowid = parse_varint(database_file)
            record = parse_record(database_file, 5)

            # Table contains columns: type, name, tbl_name, rootpage, sql
            sqlite_schema_rows.append({
                'type': record[0],
                'name': record[1],
                'tbl_name': record[2],
                'rootpage': record[3],
                'sql': record[4],
            })

        return sqlite_schema_rows

if command == ".dbinfo":
    sqlite_schema_rows = generate_schema_rows(database_file_path)
   # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    # Uncomment this to pass the first stage
    print(f"number of tables: {len(sqlite_schema_rows)}")
elif command == ".tables":
    sqlite_schema_rows = generate_schema_rows(database_file_path)
    output = ""
    for row in sqlite_schema_rows:
        tbl_name = row['tbl_name'].decode()
        if tbl_name != 'sqlite_sequence':
            output += tbl_name + ' '
    print(output)
elif command.startswith('select'):
    sql_tokens = sqlparse.parse(command)[0].tokens



    table = command.split()[-1]
    sqlite_schema_rows = generate_schema_rows(database_file_path)
    table_record = [record for record in sqlite_schema_rows if record['tbl_name'].decode() == table][0]
    table_sql = table_record['sql'].decode()

    open_paren = table_sql.find('(')
    table_sql = table_sql[open_paren + 1:len(table_sql) - 1].strip().split(',')
    columns = []
    for column_def in table_sql:
        column_name = column_def.strip().split()[0]
        columns.append(column_name)

    column_count = len(columns) 

    page_size = get_page_size(database_file_path)

    with open(database_file_path, "rb") as database_file:
        page_start = table_record['rootpage'] * page_size - page_size
        database_file.seek(page_start)
        page_header = PageHeader.parse_from(database_file)
        database_file.seek(page_start + 8) # move to the cell pointer array on the current page
        cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]

        table_rows = []
        for cell_pointer in cell_pointers:
            database_file.seek(page_start + cell_pointer)
            _number_of_bytes_in_payload = parse_varint(database_file)
            rowid = parse_varint(database_file)
            record = parse_record(database_file, column_count)

            row_dict = {}
            for i, column in enumerate(columns):
                if column == 'id':
                    row_dict[column] = rowid
                else:
                    row_dict[column] = record[i]
            table_rows.append(row_dict)

        identifiers = sql_tokens[2] 

        if type(identifiers) == Function:
            # count
            print(len(cell_pointers))
        elif type(identifiers) == Identifier:
            for row in table_rows:
                print(row[identifiers.value].decode())
        elif type(identifiers) == IdentifierList:  
            # select statement with columsn
            print(table_rows)
else:
    print(f"Invalid command: {command}")

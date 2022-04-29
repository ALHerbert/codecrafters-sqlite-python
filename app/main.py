import sys

from dataclasses import dataclass

import sqlparse 

from sqlparse.sql import IdentifierList, Function, Identifier, Comparison, Where
from .record_parser import parse_record
from .varint_parser import parse_varint

# constants
INTERIOR_INDEX_PAGE = 2
INTERIOR_TABLE_PAGE = 5
LEAF_INDEX_PAGE = 10
LEAF_TABLE_PAGE = 13


database_file_path = sys.argv[1]
command = sys.argv[2]


def get_tablename(tokens):
    # returns the first instance of identifer after the from keyword

    from_clause_reached = False 
    for token in tokens:
        if token.value == 'from':
            from_clause_reached = True
        if from_clause_reached:
            if type(token) == Identifier:
                return token.value

def get_where_clause(tokens):
    for token in tokens:
        if type(token) == Where:
            return token 

    return None

def get_comparison(tokens):
    for token in tokens:
        if type(token) == Comparison:
            return token

    return None

def get_col_and_value(token):
    return (token.left.value, token.right.value)

def get_where_condition(tokens):
    where_clause = get_where_clause(tokens)
    if where_clause:
        comparison = get_comparison(where_clause.tokens)
        if comparison:
            return get_col_and_value(comparison)

    return None

@dataclass(init=False)
class PageHeader:
    page_type: int
    first_free_block_start: int
    number_of_cells: int
    start_of_content_area: int
    fragmented_free_bytes: int
    right_most_pointer: int

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

        if instance.page_type == INTERIOR_TABLE_PAGE:
            instance.right_most_pointer = int.from_bytes(database_file.read(4), "big")

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

def read_pages(database_file, page_start, sql_tokens, column_count, page_size):
    database_file.seek(page_start)
    page_header = PageHeader.parse_from(database_file)

    table_rows = []
    if page_header.page_type == LEAF_TABLE_PAGE:
        table_rows.extend(get_table_rows(database_file, page_start, page_header, sql_tokens, column_count, page_size))
    elif page_header.page_type == INTERIOR_TABLE_PAGE:
        table_rows.extend(read_interior_page(database_file, page_start, page_header, sql_tokens, column_count, page_size))
        right_page_number = page_header.right_most_pointer * page_size - page_size
        table_rows.extend(read_pages(database_file, right_page_number, sql_tokens, column_count, page_size))

    else:
        print("Unknown page type!", page_header.page_type)

    return table_rows

def read_interior_page(database_file, page_start, page_header, sql_tokens, column_count, page_size):
    database_file.seek(page_start+12)
    cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]

    table_rows = []
    for cell_pointer in cell_pointers:
        database_file.seek(page_start + cell_pointer)
        page_number = int.from_bytes(database_file.read(4), "big") # left pointer
        _varint_integer_key = parse_varint(database_file)

        new_page_start = page_number * page_size - page_size
        table_rows.extend(read_pages(database_file, new_page_start, sql_tokens, column_count, page_size)) 

    return table_rows

def get_table_rows(database_file, page_start, page_header, sql_tokens, column_count, page_size):
    # begin function - page start, database file, page header number of cells, tokens, column count
    database_file.seek(page_start + 8) # move to the cell pointer array on the current page
    cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]

    where_condition = get_where_condition(sql_tokens)

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
        if where_condition:
            if where_condition[0] == 'id':
                if rowid == int(where_condition[1]):
                    table_rows.append(row_dict)
            else:
                if row_dict[where_condition[0]].decode() == where_condition[1].strip('"').strip("'"):
                    table_rows.append(row_dict)
        else:
            table_rows.append(row_dict)

    return table_rows

def get_table_columns(table_name, schema_rows):
    table_record = [record for record in sqlite_schema_rows if record['tbl_name'].decode() == table_name][0]
    table_sql = table_record['sql'].decode()

    open_paren = table_sql.find('(')
    table_sql = table_sql[open_paren + 1:len(table_sql) - 1].strip().split(',')
    columns = []
    for column_def in table_sql:
        column_name = column_def.strip().split()[0]
        columns.append(column_name)
    return table_record, columns

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

    table = get_tablename(sql_tokens)

    sqlite_schema_rows = generate_schema_rows(database_file_path)
    table_record, columns = get_table_columns(table, sqlite_schema_rows)
    column_count = len(columns) 

    page_size = get_page_size(database_file_path)

    with open(database_file_path, "rb") as database_file:
        page_start = table_record['rootpage'] * page_size - page_size

        # return table rows
        table_rows = read_pages(database_file, page_start, sql_tokens, column_count, page_size)
        identifiers = sql_tokens[2] 

        if type(identifiers) == Function:
            # count
            print(len(table_rows))
        elif type(identifiers) == Identifier:
            for row in table_rows:
                print(row[identifiers.value].decode())
        elif type(identifiers) == IdentifierList:  
            # select statement with columsn

            # get the names of all the columns
            columns_to_return = []
            for token in identifiers:
                if type(token) == Identifier:
                    columns_to_return.append(token.value)

            # construct a row
            for row in table_rows:
                output = ""
                for col in columns_to_return:
                    datatype = type(row[col])
                    if datatype == int:
                        output += str(row[col])
                    elif row[col] is None:
                        output += ''
                    else:
                        output += row[col].decode()
                    output += '|'
                output = output[:-1]
                print(output)
else:
    print(f"Invalid command: {command}")


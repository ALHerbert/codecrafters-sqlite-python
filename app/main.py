import re 
import sys
from dataclasses import dataclass

import sqlparse 
from sqlparse.sql import IdentifierList, Function, Identifier, Comparison, Where

from .record_parser import parse_record
from .varint_parser import parse_varint

DATABASE_HEADER_LENGTH = 100
LEAF_HEADER_LENGTH = 8
INTERIOR_HEADER_LENGTH = 12

INTERIOR_INDEX_PAGE = 2
INTERIOR_TABLE_PAGE = 5
LEAF_INDEX_PAGE = 10
LEAF_TABLE_PAGE = 13


def get_tablename(tokens):
    # returns the first instance of identifer after the from keyword

    from_clause_reached = False 
    for token in tokens:
        if token.value.lower() == 'from':
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

        if instance.page_type in [INTERIOR_TABLE_PAGE, INTERIOR_INDEX_PAGE]:
            instance.right_most_pointer = int.from_bytes(database_file.read(4), "big")
        else:
            instance.right_most_pointer = None

        return instance

def generate_schema_rows(database_path):
    with open(database_path, "rb") as database_file:
        database_file.seek(DATABASE_HEADER_LENGTH)  # Skip the header section
        page_header = PageHeader.parse_from(database_file)
        database_file.seek(DATABASE_HEADER_LENGTH + LEAF_HEADER_LENGTH)  # Skip the database header & b-tree page header, get to the cell pointer array
        
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
    database_file.seek(page_start+ INTERIOR_HEADER_LENGTH)
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
    database_file.seek(page_start + LEAF_HEADER_LENGTH) # move to the cell pointer array on the current page
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
                if row_dict[where_condition[0]] and row_dict[where_condition[0]].decode() == where_condition[1].strip('"').strip("'"):
                    table_rows.append(row_dict)
        else:
            table_rows.append(row_dict)

    return table_rows

def get_table_columns(table_name, sqlite_schema_rows):
    table_record = [record for record in sqlite_schema_rows if record['tbl_name'].decode() == table_name][0]
    table_sql = table_record['sql'].decode()

    open_paren = table_sql.find('(')
    table_sql = table_sql[open_paren + 1:len(table_sql) - 1].strip().split(',')
    columns = []
    for column_def in table_sql:
        column_name = column_def.strip().split()[0]
        columns.append(column_name)
    return table_record, columns

def read_from_index(database_file, page_number, page_size, value):
    page_start = page_number * page_size - page_size
    database_file.seek(page_start)
    page_header = PageHeader.parse_from(database_file)
    if page_header.page_type == INTERIOR_INDEX_PAGE: 
        database_file.seek(page_start + INTERIOR_HEADER_LENGTH) 
    elif page_header.page_type == LEAF_INDEX_PAGE:
        database_file.seek(page_start + LEAF_HEADER_LENGTH)
    cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]
    
   
    rowids = []
    value_less_than_first = False
    found_in_node = False
    found_last = False # a match has been found and there exists an additional nonmatch

    for i, cell_pointer in enumerate(cell_pointers):
        database_file.seek(page_start + cell_pointer)
        if page_header.page_type == INTERIOR_INDEX_PAGE: 
            left_pointer = int.from_bytes(database_file.read(4), "big")
        _number_of_bytes_in_payload = parse_varint(database_file)
        record = parse_record(database_file, 2) # number of columns in the index + the one column for rowid

        if found_in_node and record[0] != value:
            # we just passed the last matched value. go into the left pointer (which is right pointer of last match) but don't add rowid
            found_last = True
            break

        if record[0] and record[0] >= value:
            if record[0] == value: # if match found, add rowid to list and then go into left pointer 
                #go into left pointer
                found_in_node = True
                if page_header.page_type == INTERIOR_INDEX_PAGE: 
                    rowids.extend(read_from_index(database_file, left_pointer, page_size, value))
                
                rowids.append(record[1])
            else:
                if i == 0:
                    value_less_than_first = True

                if page_header.page_type == INTERIOR_INDEX_PAGE: 
                    rowids.extend(read_from_index(database_file, left_pointer, page_size, value))
                
                break
            # we're not going to find the value in the rest of the list
    if not value_less_than_first : # ignore the right pointer stuff completely if the left most pointer was used or if the value has alreayd been found in a parent node

        # if we made it through the list without finding the value, go into the right most pointer
        # we want to go into the right pointer when no matches are found or we found a match as the last item

        if not found_in_node and page_header.page_type == INTERIOR_INDEX_PAGE:
            rowids.extend(read_from_index(database_file, page_header.right_most_pointer, page_size, value))

        if found_in_node and not found_last and page_header.page_type == INTERIOR_INDEX_PAGE:
            rowids.extend(read_from_index(database_file, page_header.right_most_pointer, page_size, value))

    # can an interior node not have a right most pointer?
    return rowids

    # need to check if its a leaf node or interior node. if leaf, we don't have to search any further
def get_number_of_tables(schema_rows):
    row_count = 0
    for row in schema_rows:
        if row['type'].decode() == 'table':
            row_count += 1

    return row_count

def search_by_rowid(database_file, page_number, column_count, page_size, k, columns):


    page_start = page_number * page_size - page_size 
    database_file.seek(page_start)
    page_header = PageHeader.parse_from(database_file)

    if page_header.page_type == LEAF_TABLE_PAGE:
        return read_leaf_by_by_rowid(database_file, page_start, page_header, column_count, page_size, k, columns)
    elif page_header.page_type == INTERIOR_TABLE_PAGE:
        return read_interior_page_by_rowid(database_file, page_start, page_header, column_count, page_size, k, columns)

    else:
        print("Unknown page type!", page_header.page_type)

def read_interior_page_by_rowid(database_file, page_start, page_header, column_count, page_size, rowid, columns):
    database_file.seek(page_start+ INTERIOR_HEADER_LENGTH)
    cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]

    for cell_pointer in cell_pointers:
        database_file.seek(page_start + cell_pointer)
        page_number = int.from_bytes(database_file.read(4), "big") # left pointer
        integer_key = parse_varint(database_file)

        if rowid <= integer_key:
            # search the left
            return search_by_rowid(database_file, page_number, column_count, page_size, rowid, columns)

    # if nothing found search the right most pointer
    return search_by_rowid(database_file, page_header.right_most_pointer, column_count, page_size, rowid, columns) 

def read_leaf_by_by_rowid(database_file, page_start, page_header, column_count, page_size, k, columns):
    database_file.seek(page_start + LEAF_HEADER_LENGTH) # move to the cell pointer array on the current page
    cell_pointers = [int.from_bytes(database_file.read(2), "big") for _ in range(page_header.number_of_cells)]

    for cell_pointer in cell_pointers:
        database_file.seek(page_start + cell_pointer)
        _number_of_bytes_in_payload = parse_varint(database_file)
        rowid = parse_varint(database_file)
        record = parse_record(database_file, column_count)

        if k == rowid:

            row_dict = {}
            for i, column in enumerate(columns):
                if column == 'id':
                    row_dict[column] = rowid
                else:
                    row_dict[column] = record[i]

            return row_dict
    # since this is a leaf, if the key isn't found in this node, return None
    return None

def get_indexes(sqlite_schema_rows):
    indexes = {}
    for row in sqlite_schema_rows:
        if row['type'].decode() == 'index':
            if row['sql']:
                column = re.findall("\((.*?)\)", row['sql'].decode())[0]
                indexes[(row['tbl_name'].decode(), column)] = row['rootpage']

    return indexes

def query_index(database_file, index_rootpage, page_size, value, columns, rootpage):
    rowids = read_from_index(database_file, index_rootpage, page_size, value)
    table_rows = []

    for rowid in rowids:
        row = search_by_rowid(database_file, rootpage, len(columns), page_size, rowid, columns) 
        table_rows.append(row)

    return table_rows  

def command_dot_dbinfo(database_file_path):
    sqlite_schema_rows = generate_schema_rows(database_file_path)
   # You can use print statements as follows for debugging, they'll be visible when running tests.
    print("Logs from your program will appear here!")
    # Uncomment this to pass the first stage
    print(f"number of tables: {get_number_of_tables(sqlite_schema_rows)}")

def command_dot_tables(database_file_path):
    sqlite_schema_rows = generate_schema_rows(database_file_path)
    output = ""
    for row in sqlite_schema_rows:
        tbl_name = row['tbl_name'].decode()
        if tbl_name != 'sqlite_sequence' and row['type'].decode() == 'table':
            output += tbl_name + ' '
    print(output)

def select_statement(database_file_path):
    sql_tokens = sqlparse.parse(command)[0].tokens

    table = get_tablename(sql_tokens)

    sqlite_schema_rows = generate_schema_rows(database_file_path)
    indexes = get_indexes(sqlite_schema_rows)
    table_record, columns = get_table_columns(table, sqlite_schema_rows)
    column_count = len(columns) 

    with open(database_file_path, "rb") as database_file:
        database_file.seek(16)
        page_size = int.from_bytes(database_file.read(2), "big")

        page_start = table_record['rootpage'] * page_size - page_size

        # return table rows
        # if where clause uses index, search there instead
        where_condition = get_where_condition(sql_tokens)
        if where_condition and (table, where_condition[0]) in indexes:
            index_rootpage = indexes[table, where_condition[0]]
            table_rows = query_index(database_file, index_rootpage, page_size, where_condition[1].strip('"').strip("'").encode(), columns, table_record['rootpage'])
        else:
            table_rows = read_pages(database_file, page_start, sql_tokens, column_count, page_size)
        identifiers = sql_tokens[2] 

        if type(identifiers) == Function:
            # count
            print(len(table_rows))
        elif type(identifiers) == Identifier:
            for row in table_rows:
                if type(row[identifiers.value]) == int:
                    print(str(row[identifiers.value]))
                else:
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

if __name__ == "__main__":
    database_file_path = sys.argv[1]
    command = sys.argv[2]

    if command == ".dbinfo":
        command_dot_dbinfo(database_file_path)
    elif command == ".tables":
        command_dot_tables(database_file_path)
    elif command.lower().startswith('select'):
        select_statement(database_file_path)
    else:
        print(f"Invalid command: {command}")


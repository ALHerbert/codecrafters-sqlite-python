'''
ast = {
    'type': 'select',
    'columns': [{
        'expr': {'type': 'column_ref', column: 'id'}
        }],
    'from': [{'table': ''}],
    'where': {'type': 'binary_expr', 'operator': '=', left: {'type': 'column_ref', 'column': 'id'}, right: {'type': 'number', value: 1}} # this entire thing is a binary expression. row is compared against this. type can also be string or single quote string 
}
'''

import sqlparse 
from sqlparse.sql import IdentifierList, Function, Identifier, Comparison, Where

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

def parse(statement):
    sql_tokens = sqlparse.parse(statement)[0].tokens
    ast = {
        "type": "select",
        'from': [{"table": get_tablename(sql_tokens)}],
    }

    columns = []
    identifiers = sql_tokens[2] 
    if type(identifiers) == Function:
        # count
        columns.append({'type': 'function'})

    elif type(identifiers) == Identifier:
        columns.append({'expr': {'type': 'column_ref', 'column': identifiers.value}})
    elif type(identifiers) == IdentifierList:  
        # select statement with columsn

        # get the names of all the columns
        for token in identifiers:
            if type(token) == Identifier:
                columns.append({'expr': {'type': 'column_ref', 'column': token.value}})

    ast["columns"] = columns

    return ast

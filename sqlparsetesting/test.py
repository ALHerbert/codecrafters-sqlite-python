import sqlparse
from sqlparse.sql import IdentifierList, Function, Identifier, Where, Comparison
tokens = sqlparse.parse('select name, age from people where id = 1')[0].tokens

def get_tablename(tokens):
    # returns the first instance of identifer after the from keyword

    from_clause_reached = False 
    for token in tokens:
        if token.value == 'from':
            from_clause_reached = True
        if from_clause_reached:
            if type(token) == Identifier:
                return token 

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


# i need to get the left side (column_name)
# the right side actual value
# and the comparison operator

#print(get_tablename(tokens))
#token = get_where_condition(tokens)
#other = get_comparison(token.tokens)
#if other:
#    print(get_col_and_value(other))
print(get_where_condition(tokens))

import json


class ESObj(object):
    """
    Top level ESObj. 

    Args:
        obj: dict/json
        
    Methods:
        to_sql(self): translate to sql statement. 
    """
    def __init__(self, obj):
        self.obj = obj
        self.parse(self.obj)

    def parse(self, obj):
        self.child_obj = self.get_class(obj)
        
    def get_class(self, obj):
        if 'bool' in obj:
            return Bool(obj['bool'])
        elif 'term' in obj:
            return Term(obj['term'])
        elif 'terms' in obj:
            return Terms(obj['terms'])
        elif 'exists' in obj:
            return Exists(obj['exists'])
        elif 'nested' in obj:
            return Nested(obj['nested'])
        else:
            raise Exception('ESObj get class error: ', obj)
    
    def to_sql(self):
        return self.child_obj.to_sql()


class Bool(ESObj):
    """
    Bool obj, only support 'filter', 'must', 'must_not', 'should'.

    For filter/must, connection is ' AND '
    For must_not, connection is ' NOT (stmt1 AND stmt2 AND ...)'
    For should, connection is ' OR '
    """
    def __init__(self, obj):
        super().__init__(obj)

        self.collection = []
        self.type = None
        
    def parse(self, obj):
        for tp in ('filter', 'must', 'must_not', 'should'):
            if tp in obj:
                self.type = tp
                for item in obj[tp]:
                    self.collection.append(self.get_class(item))
                return
        raise Exception('ES Bool obj parse error: ', obj)


    def to_sql(self):
        if self.type in ('filter', 'must'):
            inner_sql = ' AND '.join([item.to_sql() for item in self.collection])
            return f"({inner_sql})"
        elif self.type == 'must_not':
            inner_sql = ' AND '.join([item.to_sql() for item in self.collection])
            return f"NOT ({inner_sql})"
        elif self.type == 'should':
            inner_sql = ' OR '.join([item.to_sql() for item in self.collection])
            return f"({inner_sql})"
        else:
            raise Exception('ES Bool obj to sql error: Type Error. ', self.obj)

        
class Term(ESObj):
    """
    term obj. 
    """
    def parse(self, obj):
        keys = list(obj.keys())
        self.field = keys[0]
        if isinstance(obj[self.field], list):
            self.value = obj[self.field]
        else:
            self.value = obj[self.field]['value']

    def to_sql(self):
        # if value is number
        if isinstance(self.value, int):
            return f"({self.field} = {self.value})"
        elif isinstance(self.value, list):
            # transfer to (value1, value2, value3)
            value_str = ', '.join([f"'{v}'" for v in self.value])
            return f"({self.field} IN ({value_str}))"
        else:
            return f"({self.field} = '{self.value}')"


## TODO: complete for border case
class Terms(Term):
    """
    (Undone yet) Terms obj, extend from Term.
    """
    def __init__(self, obj):
        super().__init__(obj)


class Exists(ESObj):
    def parse(self, obj):
        self.field = obj['field']

    def to_sql(self):
        return f"({self.field} IS NOT NULL)"


# TODO: The SQL result is not correct yet. 
class Nested(ESObj):
    """
    (Undone yet) Nested statement. 
    """
    def parse(self, obj):
        self.path = obj['path']
        self.query = obj['query']

        self.query = ESObj(self.query)

    def to_sql(self):
        return f"({self.path} {self.query.to_sql()})"


''' Test Codes below. '''
if __name__ == '__main__':
    # read from json file ./es_exp_for_test.json
    with open('./es_exp_for_test.json', 'r') as f:
        es_exp = json.load(f)

    es_obj = ESObj(es_exp)
    print(es_obj, es_obj.child_obj.collection)
    sql = es_obj.to_sql()
    print(sql)

    # save result， with formmating style
    with open('./es_exp_for_test1.sql', 'w') as f:
        f.write(sql)

        # format sql, add index space



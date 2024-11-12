import json
from collections import defaultdict

class ESRule(dict):
    """
    Define rule for es2sql.
        {
            'ignore':{
                'field': True,
                ...
            },
            'field_map':{
                'from':'to',
                ...
            },
            'value_map':{
                'field':{'from':'to', ...},
            },
            'eq2like':{
                'field': True,
                ...
            },
            'eq2reg':{
                'field': ['prefix', 'suffix'],
                ...
            },
            'in2like':{
                'field': True,
                ...
            },
            'nn2empty':{
                'field': True,
                ...
            },
        }
    """
    def __init__(self, ignore=defaultdict(), 
                        field_map=defaultdict(), 
                        value_map=defaultdict(), 
                        eq2like=defaultdict(), 
                        eq2reg=defaultdict(), 
                        in2like=defaultdict(), 
                        nn2empty=defaultdict()):
        self.ignore = ignore
        self.field_map = field_map
        self.value_map = value_map
        self.eq2like = eq2like
        self.eq2reg = eq2reg
        self.in2like = in2like
        self.nn2empty = nn2empty


class ESObj(object):
    """
    Top level ESObj. 

    Args:
        obj: dict/json
        rule: ESRule
        
    Methods:
        to_sql(self): translate to sql statement. 
    """
    def __init__(self, obj: dict, rule=ESRule()):
        self.obj = obj
        self.rule = rule
        self.parse(self.obj, rule)

    def parse(self, obj, *args, **kwargs):
        self.child_obj = self.get_class(obj, *args, **kwargs)
        
    def get_class(self, obj, *args, **kwargs):
        # self.rule = self.rule if self.rule else rule
        for tp in ('bool', 'term', 'terms', 'exists', 'nested'):
            if tp in obj:
                return eval(tp.title())(obj[tp], *args, **kwargs) # Instanlize the class by class name.
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
    def __init__(self, *args, **kwargs):
        self.collection = []
        self.type = None

        super().__init__(*args, **kwargs)
        
    def parse(self, obj: dict, *args, **kwargs):
        for tp in ('filter', 'must', 'must_not', 'should'):
            if tp in obj:
                self.type = tp
                for item in obj[tp]:
                    self.collection.append(self.get_class(item, *args, **kwargs))
                return
        raise Exception('ES Bool obj parse error: ', obj)


    def to_sql(self):
        if self.type in ('filter', 'must'):
            ''' (stmt1 AND stmt2 AND ...) '''
            inner_sql = ' AND '.join([item.to_sql() for item in self.collection])
            return f"({inner_sql})"
        elif self.type == 'must_not':
            ''' NOT (stmt1 AND stmt2 AND ...) '''
            inner_sql = ' AND '.join([item.to_sql() for item in self.collection])
            return f"NOT ({inner_sql})"
        elif self.type == 'should':
            ''' (stmt1 OR stmt2 OR ...) '''
            inner_sql = ' OR '.join([item.to_sql() for item in self.collection])
            return f"({inner_sql})"
        else:
            raise Exception('ES Bool obj to sql error: Type Error. ', self.obj)

        
class Term(ESObj):
    def __init__(self, *args, **kwargs):
        self.field = None
        self.value = None
        self.final_field = None

        super().__init__(*args, **kwargs)

    """
    term obj. 
    """
    def parse(self, obj: dict, *args, **kwargs):
        """
        Handle value type of int, list or str
        Follow the rule to rename the field
        """
        keys = list(obj.keys())
        self.field = keys[0]
        if isinstance(obj[self.field], list):
            '''  [x1, x2, x3, ...]  '''
            self.value = obj[self.field]
        else:
            ''' {value: x} '''
            self.value = obj[self.field]['value']

        ''' Rename the field by field_map '''
        if self.rule.field_map and self.rule.field_map.get(self.field, None):
            self.final_field = self.rule.field_map.get(self.field, None)
        else:
            self.final_field = self.field

        ''' Rename the value by value_map '''
        if self.rule.value_map and self.rule.value_map.get(self.field, None):
            if isinstance(self.value, list):
                self.value = [self.rule.value_map.get(self.field, None)[v] for v in self.value]
            else:
                self.value = self.rule.value_map.get(self.field, None)[self.value]

    def to_sql(self):
        """
        Handle value type of int, list or str
        """
        ''' ignore the field '''
        if self.rule.ignore and self.rule.ignore.get(self.field, None):
            return '(1=1)'

        ''' IN operator '''
        if isinstance(self.value, list):
            # IN to LIKE operator
            if self.rule.in2like and self.rule.in2like.get(self.field, None):
                ''' [OR] field LIKE %value1% ... '''
                return "(" + \
                    " OR ".join([f"({self.final_field} LIKE '%{v}%')" for v in self.value]) + \
                ")"

            # IN operator
            return f"({self.final_field} IN (" + \
                                                ', '.join([f"'{v}'" for v in self.value]) + \
                                            "))"
        
        ''' EQ operator '''
        # int value
        if isinstance(self.value, int):
            return f"(CAST({self.final_field} AS UInt8) = {self.value})"
        
        # eq to like
        if self.rule.eq2like and self.rule.eq2like.get(self.field, None):
            return f"({self.final_field} LIKE '%{self.value}%')"
        
        # eq to regexp
        if self.rule.eq2reg and self.rule.eq2reg.get(self.field, None):
            # '(?i).*name.*')
            return f"({self.final_field} REGEXP '(?i).*{self.value}.*')"

        # string value
        return f"({self.final_field} = '{self.value}')"


## TODO: complete for border case
class Terms(Term):
    """
    Terms obj, now extend from Term.
    """
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class Exists(ESObj):
    def parse(self, obj: dict, *args, **kwargs):
        self.field = obj['field']

    def to_sql(self):
        if self.rule.nn2empty and self.rule.nn2empty.get(self.field, None):
            return f"(NOT {self.field} = '')"
        return f"({self.field} IS NOT NULL)"


# TODO: May not correct.
class Nested(ESObj):
    """
    Nested statement. 
    """
    def parse(self, obj: dict, *args, **kwargs):
        self.path = obj['path']
        self.query = obj['query']

        self.query = ESObj(self.query, *args, **kwargs)

    def to_sql(self):
        return f"({self.query.to_sql()})"


''' Test Codes below. '''
if __name__ == '__main__':
    # read from json file ./es_exp_for_test.json
    # with open('./es_exp_for_test.json', 'r') as f:
    #     es_exp = json.load(f)

    es_exp = '''
...
'''

    es_obj = ESObj(json.loads(es_exp))
    sql = es_obj.to_sql()
    print(sql)

    # save result， with formmating style
    # with open('./es_exp_for_test1.sql', 'w') as f:
    #     f.write(sql)

        # format sql, add index space


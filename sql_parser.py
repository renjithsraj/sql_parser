import json

TableNotfoundMsg = "Table name is not found in nodes and not associated any edges"

class TableNotFoundException(Exception):
    """
        Custom Exception for Tablename not found error
    """
    pass

class Parser():

    def __init__(self, path) -> None:
        self.path = path
        self.query_list = []

    def read_json(self):
        """
            This function is read the data from 
            json file and return json data
        """
        json_data = None
        with open(self.path) as file:
            json_data = json.loads(file.read())
        return json_data

    def parse_list(self, arg_list, fields=True):
        """
            desc : Converting list into coma seperated string 
            arg_list: arg_list expecting any list of items
            response: return coma seperated string
        """
        if fields == True:
            if '*' in arg_list:
                arg_list = ['*']
            else:
                arg_list = [f"`{arg}`"  for arg in arg_list]
        return ",".join(arg_list)
    
    def find_to_table(self, edges, to_table):
        return next(filter(lambda x: x['to'] == to_table, edges),dict(to=to_table))

    def prepare_query_string(self, cols, table_name, extra_args):
        """
            desc: Prepare Query string for the arguments
            response (eg:): SELECT id,name from users where id=1
        """
        _query = f"SELECT {cols} FROM {table_name} {extra_args}"
        return _query
    
    def prepare_extra_args(self, operations, join_operator='AND'):
        """
            This function is use to prepare extra arguments,
            ie: this function will help us to apply where clause condition,
            order by and limit actions.
            request will be in list format[opearations:[]], function will iterate
            over the list and return the final condition.
        """
        # Initialize General Parameter
        where_condition_list = []
        sorting_condition_list = {'ASC': [], 'DESC': []}
        extra_args = ""
        where_condition, order_condition, limit_offset_str = "", "", ""
    
        for operation in operations:
            # Where / Filter condition
            if operation.get('operator'):
                """ 
                    Where/filter operation with given fields
                    eg: {'field_name': 'age', 'operator': '>', 'value': '18'}
                    output: where age > 18
                    
                """
                filter_column = operation.get('field_name')
                filter_operatior = operation.get('operator')
                filter_value = operation.get('value') 
                where_condition_list.append(f"`{filter_column}` {filter_operatior} {filter_value}")
            # Order BY operation
            if operation.get('order'):
                """
                    Order by Operation for given fileds.
                    eg: { "target": "age", "order": "ASC"}
                    output : ORDER BY age ASC
                """
                sorting_condition_list[operation.get('order')].append(f"`{operation.get('target')}`")
            # Limit operation
            if operation.get('limit'):
                limit, offset= operation.get('limit'), operation.get('offset', 0)
                limit_offset_str = f"LIMIT {limit} {offset}"
                
        # Processing filter condition(where clause) 
        if len(where_condition_list) > 0:
            condition = f" {join_operator} ".join(where_condition_list)
            where_condition = f" WHERE {condition}"
        # Sorting 
        if (len(sorting_condition_list.get('ASC')) > 0) or (len(sorting_condition_list.get('DESC')) > 0):
            order_by_list = []
            asc_list = sorting_condition_list.get('ASC')
            desc_list = sorting_condition_list.get('DESC')
            # Processing ASC Data
            if len(asc_list) > 0:
                asc_str = f'{" , ".join(asc_list)} ASC'
                order_by_list.append(asc_str)
            # Processing DESC Data
            if len(desc_list) > 0:
                desc_str = f'{" , ".join(desc_list)} DESC'
                order_by_list.append(desc_str)
            order_condition = f"ORDER BY {' , '.join(order_by_list)}"
        
        # Final condition, Appending where, order, limit condition in final condition parameter
        # And returing the result to main function
        final_condition = f"{where_condition} {order_condition} {limit_offset_str}"
        return final_condition
    
    def transformation_func(self, operations):
        """
            Applying aggrigation/transformation functions for 
            the requested fields
        """

        transformation_list, transformation_string = [], None
        for operation in operations:
            if operation.get('transformation'):
                trans_func = operation.get('transformation')
                field_name = operation.get('column')
                transformation_string = f"{trans_func}(`{field_name}`) as `{field_name}`"
                transformation_list.append(transformation_string)

        if len(transformation_list) > 0:
            transformation_string = " , ".join(transformation_list)
        return transformation_string
    
    def input_parser(self, fields, table_name, operations, key, 
        join_operator='AND'):
        """
            desc: to handle operation type='INPUT'
            resp: query string
        """
        extra_args = self.prepare_extra_args(operations, join_operator)  
        query_data = {
            'cols': fields,
            'table_name': table_name,
            'extra_args': extra_args
        }
        query = self.prepare_query_string(**query_data)
        _query = f"{key} AS ({query})"
        return _query
    
    def filter_parser(self, fields, table_name, operations, key, 
        join_operator='AND'):
        """
            desc: to handle operation type='FILTER'
            resp: query string
        """
        extra_args = self.prepare_extra_args(operations, join_operator)         
        query_data = {
            'cols': fields,
            'table_name': table_name,
            'extra_args': extra_args
        }
        query = self.prepare_query_string(**query_data)
        query = f"{key} AS ({query})"
        return query

    def sort_parser(self, fields, table_name, operations, key, 
        join_operator='AND'):
        """
            desc: to handle operation type='SORT'
            resp: query string
        """
        extra_args = self.prepare_extra_args(operations, join_operator)
        query_data = {
            'cols': fields,
            'table_name': table_name,
            'extra_args': extra_args
        }
        query = self.prepare_query_string(**query_data)
        query = f"{key} AS ({query})"
        return query
    
    def trasformation_parser(self, fields, table_name, operations, key, 
        join_operator='AND'):
        """
            desc: to handle operation type='SORT'
            resp: query string
        """
        extra_args = self.prepare_extra_args(operations, join_operator)
        query_data = {
            'cols': fields,
            'table_name': table_name,
            'extra_args': extra_args
        }
        query = self.prepare_query_string(**query_data)
        query = f"{key} AS ({query})"
        return query
    
    def output_parser(self, fields, table_name, operations, key, 
        join_operator='AND'):
        """
            desc: to handle operation type='SORT'
            resp: query string, final output query
        """
        extra_args = self.prepare_extra_args(operations, join_operator)
        query_data = {
            'cols': fields,
            'table_name': table_name,
            'extra_args': extra_args
        }
        query = self.prepare_query_string(**query_data)
        query = f"{key} AS ({query})"
        output_query = f"SELECT * FROM {key};"
        return query, output_query

    def parse(self, json_data):
        # Read Json Data
        # json_data = self.read_json()
        for data in json_data['request_data']:
            nodes =  data['nodes']
            edges = data['edges']
            final_query_list = []
            output_query = []
            # Itrate the node and process the Query
            for node in nodes:
                transform_obj = node['transformObject']
                table_name = transform_obj.get('tableName', None)
                operations = transform_obj.get('operations', [])
                join_operator = transform_obj.get('joinOperator', 'AND')

                key = node['key']
                if not table_name:
                    _edge = self.find_to_table(edges, key)
                    table_name = _edge.get('from')
                table_name = f"`{table_name}`"
                fields = self.parse_list(transform_obj.get('fields'))
                extra_cols = self.transformation_func(operations)
                if extra_cols:
                    fields = f"{fields}, {extra_cols}"
                
                if not table_name:
                    raise TableNotFoundException(TableNotfoundMsg)
                
                # Select Parser based on type
                # He I have created individual functions for handling operations
                if node['type'] == 'INPUT':
                    query = self.input_parser(fields, table_name, operations, key)
                    final_query_list.append(query)                
                
                elif node['type'] == 'FILTER':
                    query = self.filter_parser(fields, table_name, operations, key, 
                        join_operator=join_operator)
                    final_query_list.append(query)
                
                elif node['type'] == 'SORT':
                    query = self.sort_parser(fields, table_name, operations, 
                        key, join_operator)
                    final_query_list.append(query)
                
                elif node['type'] == 'TEXT_TRANSFORMATION':
                    query = self.trasformation_parser(fields, table_name, operations, 
                        key, join_operator)
                    final_query_list.append(query)
                
                if node['type'] == 'OUTPUT':
                    query, output_query = self.output_parser(fields, table_name, operations, key, 
                        join_operator)
                    final_query_list.append(query)
                    output_query = output_query
            final_query = f' WITH {self.parse_list(final_query_list, fields=False)} {output_query}'
            self.query_list.append(final_query)
        
        # Final querylist returns
        return self.query_list
    
    def to_sql(self, return_path='result_sql.sql'):
        """
            desc: Read the json file and parse the data
            input -> return_path:- export path for parsed query
            output -> return sql file
        """
        json_data = self.read_json()
        self.parse(json_data)
        sql_string = "\n".join(self.query_list)
        with open(return_path, "w+") as file:
            file.write(sql_string)
        return return_path


if __name__ == "__main__":
    print(f"{'*'*20} Parser started {'*'*20}")
    try:
        path = r"D:\working_directory\modeling-test-master\modeling-test-master\request-data.json"
        return_path = 'sample_file.sql'
        parser = Parser(path=path)
        export_path = parser.to_sql(return_path)
        print(f"{'*'*20} Parser  Completed & filepath -> {export_path} {'*'*20}")
    except Exception as e:
        raise Exception(f"Error -> {str(e)}")



        
    




                    
            
                
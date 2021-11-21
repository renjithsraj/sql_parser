#### Parser ####
Converting given request-data.json file into sql parser.

Module Name is: sql_parser

#### How to Use
```python
    from sql_parser import Parser
    parser = Parser(path=path) # Create
    export_path = parser.to_sql(return_path)
```
path        -> Json path for request_data
return_path -> export file pathm to_sql function will export the requested 
            json file into Sql file

#### Sample Test Case ####
```python
   from sql_parser import Parser
   print(f"{'*'*20} Parser started {'*'*20}")
    try:
        path = r"D:\working_directory\modeling-test-master\modeling-test-master\request-data.json"
        return_path = 'sample_file.sql'
        parser = Parser(path=path)
        export_path = parser.to_sql(return_path)
        print(f"{'*'*20} Parser  Completed & filepath -> {export_path} {'*'*20}")
    except Exception as e:
        raise Exception(f"Error -> {str(e)}")
```

### Points
 - I have update few changes on the request-data.json, Please find the updated json file in the directory
 - Please let me know if you required any clarification
 - The given code is tested with window 10 OS. My ubunutu system had some technical issues.
  
            







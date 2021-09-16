import pydantic
from pydantic import ValidationError
import boto3
import json
from typing import List, Optional
from pydantic import BaseModel
import uuid
from boto3.dynamodb.conditions import Key

def put_item(obj, table):
    return table.put_item(Item=obj)

def get_item(obj, table):
    return table.get_item(Key=obj)

def get_items(table_name, table):
    return table.scan({TableName: table_name})

def update_item(obj, table):
    return table.update_time(Key=obj)

def delete_item(obj, table):
    return table.delete_item(Key=obj)

class EmployeeCreate(BaseModel):
    full_name: str
    home_phone: str
    cell_phone: str
    email_address: str
    social_security_number_or_government_id: str
    birth_date: str
    martial_status: str
    spouse_name: str
    spouse_employer: str
    spouse_work_phone: str

class EmployeeUpdate(BaseModel):
    id: str
    full_name: Optional[str]
    home_phone: Optional[str]
    cell_phone: Optional[str]
    email_address: Optional[str]
    social_security_number_or_government_id: Optional[str]
    birth_date: Optional[str]
    martial_status: Optional[str]
    spouse_name: Optional[str]
    spouse_employer: Optional[str]
    spouse_work_phone: Optional[str]
    
def get_update_params(body: dict):
    update_expression = ["set "]
    update_values = dict()

    for key, val in body.items():
        update_expression.append(f" {key} = :{key},")
        update_values[f":{key}"] = val

    return "".join(update_expression)[:-1], update_values
    
def validate_obj(valid_class, obj):
    result = True
    try:
        valid_class(**obj)
    except ValidationError as err:
        result = err
    return result


####### HANDLER ########
def lambda_handler(event, context):
    '''Provide an event that contains the following keys:

      - operation: one of the operations in the operations dict below
      - tableName: required for operations that interact with DynamoDB
      - payload: a parameter to pass to the operation being performed
    '''
    #########################
    # Change based on the requirements
    table_name = "Employees"
    resource = "employees"
    index_name = "id" # Please follow this convention when create primary key in DynamoDB
    create_valid_class = EmployeeCreate
    update_valid_class = EmployeeUpdate
    #########################
    
    
    status_code = 200;
    table = boto3.resource('dynamodb').Table(table_name)
    request = event.get('httpMethod') + " " + event.get("resource")
    print(request) # logging purpose
    
    body = ""
    try:
        if request == f"PUT /{resource}": # CREATE OR UPDATE item 
            obj = json.loads(event.get('body'))
            
            # If the obj does not have id field => create request
            if not obj.get('id'):
                obj['id'] = str(uuid.uuid4())
                is_valid = validate_obj(create_valid_class, obj)
                  # if obj is valid, check == True since the is_valid func return either True of object
                if is_valid == True:
                    body = table.put_item(Item=obj)
                else:
                    body = is_valid
            else:
                is_valid = validate_obj(update_valid_class, obj)
                if is_valid == True:
                    # delete the id attribute since it is a primary key
                    id = obj['id']
                    del obj["id"]
                    update_expression, update_values = get_update_params(obj)
                    body = table.update_item(
                        Key={"id": id},
                        UpdateExpression=update_expression,
                        ExpressionAttributeValues=update_values,
                        ReturnValues="UPDATED_NEW"
                    )
                else:
                    body = is_valid
            
        elif request == f"GET /{resource}" + "/{id}":
            id = event.get('pathParameters').get('id')
            body = table.query(KeyConditionExpression=Key('id').eq(id))
        
        elif request == f"DELETE /{resource}" + "/{id}":
            id = event.get('pathParameters').get('id')
            table.delete_item(Key={'id': id})
            body = f"Delete {resource} with id: {id}"
            
        elif request == f"GET /{resource}/search":
            # print(event)
            # Set up paginator
            email_address = event.get('queryStringParameters').get('email_address')
            op = event.get('queryStringParameters').get('op')
            page_size = int(event.get('queryStringParameters').get('page_size', 10))
            page = int(event.get('queryStringParameters').get('page', 1))
            
            # Modify the expression name and value depends on the task
            response = table.scan(
                ExpressionAttributeNames={
                    "#email_address": "email_address",
                },
                ExpressionAttributeValues={
                    ":email_address": email_address
                },
                FilterExpression="#email_address = :email_address"
            )
            data = response['Items']
            print(type(data))
            while 'LastEvaluatedKey' in response:
                response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
                data.extend(response['Items'])
            
            total_items = len(data)
            
            body = {
                'Items': data[(page-1)*page_size:(page-1)*page_size + page_size],
                'page': page, 
                'page_size': page_size,
                'total_page': total_items // page_size + 1
            }
            
        elif request == f"GET /{resource}":
            body = table.scan()
        else:
            raise ValueError(f'Unrecognized request "{request}"')
    except ValueError as err:
        status_code = 400;
        body = err.message;
    finally:
        if isinstance(body, ValidationError):
            body = body.json()
        else:
            body = json.dumps(body)
    return {
            'statusCode': status_code,
            'body': body,
            'headers': {
                "Access-Control-Allow-Headers" : "Content-Type",
                "Access-Control-Allow-Origin": "*",
                "Access-Control-Allow-Methods": "OPTIONS,POST,GET,PUT,DELETE"
            }
    }


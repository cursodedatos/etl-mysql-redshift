import json
import pymysql
import datetime
import boto3
import time


def lambda_handler(event, context):

    # Extract
    connection = pymysql.connect(host='xxx.xxx.us-east-1.rds.amazonaws.com',
                                 user='admin',
                                 password='pass',
                                 database='database')
    
    cur = connection.cursor(pymysql.cursors.SSCursor)  
    #cur = db.cursor(pymysql.cursors.DictCursor)
    sql = "SELECT * FROM bank LIMIT 100;"
    cur.execute(sql)
    
    now = datetime.datetime.now()
    timestamp = now.strftime("%Y-%m-%d %H:%M:%S" )
    rows_to_insert = [] 
    # Transform
    line_count = 0
    batch_size = 40
    client = boto3.client('redshift-data') 
    for row in cur:
        line_count += 1
        SQL_command = "INSERT INTO dev.public.bank VALUES ({},'{}',{},'{}');".format(line_count,row[1],row[2],timestamp)
        rows_to_insert.append(SQL_command)

    # Insert              
    array_lenght = len(rows_to_insert)    
    for i in range(0,array_lenght,batch_size):
        if i >= batch_size:
            print (i-batch_size,i)
            load_redshift(rows_to_insert[i-batch_size:i],client)
            last_count = i


    remain_data = array_lenght - last_count
    print ("remain_data",remain_data)
    if remain_data > 0:
        print (last_count,array_lenght)
        load_redshift(rows_to_insert[last_count:array_lenght],client)    
        
            
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


def load_redshift(rows_to_insert,client):

    response = client.batch_execute_statement(
        WorkgroupName ='default-workgroup',
        Database = 'dev',
        Sqls = rows_to_insert,
        SecretArn = 'arn:aws:secretsmanager:us-east-1:799412981296:secret:redshift-Imiurq'
        )
    print (response)
    #time.sleep(1)
    #response_query = client.describe_statement(
    #    Id = response['Id']
    #    )
    #print (response_query )    

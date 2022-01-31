import json, csv
import boto3
import mysql.connector
import random
import pymysql

key = 'sales_04215.csv'
bucket = 'ketan-final-task-1'
s3_resource = boto3.resource('s3')
s3_object = s3_resource.Object(bucket, key)
queue=boto3.client('sqs')
queue_url=queue.get_queue_url(QueueName='final-task-queue')['QueueUrl']
# print(f"----------------------- sqs name : {queue}, sqs url : {queue_url}")

def lambda_handler(event, context):
    # TODO implement
    try:
        dbFlag = "False"
        # Reading csv data 
        data = s3_object.get()['Body'].read().decode('utf-8').splitlines()
        lines = csv.reader(data)
        headers = next(lines)
        # Connection with RDS database
        my_connection = mysql.connector.connect(
        host="project.c0vrpj3mmoqn.ap-south-1.rds.amazonaws.com",
        user="admin",
        passwd="Pass_123",
        database="sales"
        )
        cursor = my_connection.cursor()
        for row in lines:
            Region = row[0]
            Country = row[1]
            Item_type = row[2]
            Sales_Channel = row[3]
            Order_Priority = row[4]
            Order_Date = row[5]
            Order_ID = int(row[6])
            Ship_Date = row[7]
            Units_Sold = row[8]
            Unit_Price = row[9]
            Unit_Cost = row[10]
            Total_Revenue = row[11]
            Total_Cost = row[12]
            Total_Profit = row[13]
            cursor.execute("""INSERT INTO sales_copy_1 VALUES ("%s", "%s", "%s", "%s", "%s", "%s", "%d", "%s", "%s", "%s", "%s", "%s", "%s", "%s")"""
                            % (Region, Country, Item_type, Sales_Channel, Order_Priority, Order_Date, Order_ID, Ship_Date, Units_Sold, Unit_Price, Unit_Cost,
                               Total_Revenue, Total_Cost, Total_Profit));
            my_connection.commit();
        print("--------------------- Done")
        dbFlag = "True"
        write_resp=queue.send_message(QueueUrl=queue_url,MessageBody=dbFlag)
        print("written to quere response",write_resp)
        return {
        'statusCode': 200,
        'body': json.dumps("True")
        }
    except mysql.connector.Error as e:  
        print("Here")
        print("Error reading data from MySQL table", e)
    finally:
        if my_connection.is_connected():
            my_connection.close()
            cursor.close()
            print("MySQL connection is closed")

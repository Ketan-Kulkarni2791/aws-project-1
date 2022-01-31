import json, boto3
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    # TODO implement
    if event['Records'][0]['body'] == "True":
        
        SENDER = "ketankulkarni2791@gmail.com"
        RECIPIENT = "ketankulkarni2791@gmail.com"
        
        AWS_REGION = "ap-south-1"
        
        SUBJECT = "Status Updates For CSV Upload"
        
        BODY_TEXT = (
                        "Hello, your CSV data has been inserted into RDS Database and Reports are generated and saved as well!!!"
                    )
                    
        BODY_HTML = """
                    <html>
                        <head></head>
                        <body>
                            <h1>Hello,</h1>
                            <p>
                                your CSV data has been inserted into RDS Database and Reports are generated and saved as well!!!
                            </p>
                        </body>
                    </html>
        """
        
        CHARSET = "UTF-8"
        
        client = boto3.client('ses', region_name=AWS_REGION)
        
        try:
            response = client.send_email(
                    Destination={
                        'ToAddresses': [
                                RECIPIENT,
                            ],
                    },
                    Message={
                        'Body': {
                            'Html': {
                                'Charset': CHARSET,
                                'Data': BODY_HTML
                            },
                            'Text': {
                                'Charset': CHARSET,
                                'Data': BODY_TEXT
                            },
                        },
                        'Subject': {
                            'Charset': CHARSET,
                            'Data': SUBJECT   
                        },
                    },
                    Source=SENDER
                )
        except ClientError as e:
            print(e.response['Error']['Message'])
        else:
            print("Email sent successfully! MessageID : ")
            print(response['MessageId'])

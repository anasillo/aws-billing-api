import datetime
import boto3
import json
import pandas as pd
import pytz
import pyarrow
import os
from google.cloud import bigquery
from google.cloud import secretmanager

def access_secret_version(secret_id):
    client = secretmanager.SecretManagerServiceClient()
    
    response = client.access_secret_version(name="projects/965932955678/secrets/{secret_id}/versions/latest" .format (secret_id=secret_id))
    secret = response.payload.data.decode("UTF-8")
    
    return secret

def awsClient (module):
    
    client = boto3.client(
        module,
        aws_access_key_id = access_secret_version("aws_accesskey_billing"),
        aws_secret_access_key = access_secret_version("aws_secretkey_billing"))

    return client

def time (add_time):
    time = datetime.datetime(   
        year=datetime.datetime.now().year, 
        month=datetime.datetime.now().month + add_time, 
        day=1
        ).strftime("%Y-%m-%d")
    return time

def getAccounts():
    accounts = []
    client = awsClient ('organizations')
    paginator = client.get_paginator('list_accounts')
    response_iterator = paginator.paginate()
    for response in response_iterator:
        for acc in response['Accounts']:
            accounts.append([
                acc['Id'],
                acc['Name'],
                acc['Email'],
                acc['Status']
            ])

    return accounts

def getBilling(account_info):
    
    start_time, end_time = time (-1), time (0)
        
    billing = []
    
    for account in range(len(account_info)):

        client = awsClient ('ce')

        response = client.get_cost_and_usage(
            TimePeriod={
                'Start': start_time,
                'End': end_time

            },
            Filter={
                'Dimensions': {
                    'Key': 'LINKED_ACCOUNT',
                    'Values': [
                        account_info[account][0],
                    ],
                    'MatchOptions': [
                        'EQUALS',
                    ]
                }
            },
            Granularity='MONTHLY',
            GroupBy=[
                {
                    'Type': 'DIMENSION',
                    'Key': 'SERVICE'
                }

            ],
            Metrics=[
                'UnblendedCost'
            ]
        )

        for month in response['ResultsByTime']:
            for key in month['Groups']:

                billing.append([
                                account_info[account][0], account_info[account][1], account_info[account][2], account_info[account][3],
                                str(month['TimePeriod']['Start'])[:7],
                                str(key['Keys']).replace("[", "").replace("]", "").replace("'",""), 
                                float(key['Metrics']['UnblendedCost']['Amount']),
                                key['Metrics']['UnblendedCost']['Unit']
                                ])
    return billing

def main (event, context):

    df = pd.DataFrame.from_dict ( getBilling (getAccounts ()) )
    df.columns = [ 'Id', 'Customer', 'Email', 'Status', 'StartDate', 'Service', 'Amount', 'Unit' ]

    df.loc [df.Service == 'Refund', ['Id']] = '0000000000 - ' + df['Customer']
    df.loc [df.Service == 'Refund', ['Customer', 'Email']] = 'Refund', ''
    
    client = bigquery.Client()

    table_id = "bghtp-datalake.billing_api.aws"

    dataframe = pd.DataFrame(
        df,
        columns=[
            'Id',
            'Customer',
            "Email",
            "Status",
            "StartDate",
            "Service",
            "Amount",
            "Unit" 
        ],
    )

    job_config = bigquery.LoadJobConfig(
        # schema=[
        #     bigquery.SchemaField("Id", bigquery.enums.SqlTypeNames.STRING),
        #     bigquery.SchemaField("Customer", bigquery.enums.SqlTypeNames.STRING),
        # ],

        write_disposition="WRITE_APPEND",
    )

    job = client.load_table_from_dataframe(
        dataframe, table_id, job_config=job_config
    )  # Make an API request.
    job.result()  # Wait for the job to complete.

    table = client.get_table(table_id)
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
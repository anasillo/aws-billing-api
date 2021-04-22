import boto3
import json
import pandas as pd
import pytz
import pyarrow
import os
from datetime import datetime, timedelta
from google.cloud import bigquery
from google.cloud import secretmanager
from google.cloud import storage

def access_secret_version (secret_id):
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

def time ():
    end_time = datetime(year=datetime.now().year, month=datetime.now().month, day=1)
    start_time = end_time - timedelta (days = 1) 
    start_time = datetime(year=start_time.year, month=start_time.month, day=1)
    return start_time.strftime("%Y-%m-%d"), end_time.strftime("%Y-%m-%d")

def getAccounts ():
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

def getBilling (account_info):
    
    start_time, end_time = time()
        
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

def loading_bigquery (df):
    
    client = bigquery.Client()
    table_id = "bghtp-datalake.billing_api.aws"

    dataframe = pd.DataFrame(
        df,
        columns=[
            'Id',
            'Customer',
            "Email",
            "Status",
            "Date",
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

def short_services ():
    services = ['AWS Key Management Service', ' Amazon EC2 Container Registry (ECR)', 
        ' Amazon Elastic Compute Cloud - Compute', ' Amazon Elastic Container Service', 
        ' Amazon Elastic Container Service for Kubernetes', ' Amazon Elastic File System', 
        ' Amazon Elastic Load Balancing', ' Amazon Elasticsearch Service', ' Amazon Relational Database Service', 
        ' Amazon Simple Email Service', ' Amazon Simple Notification Service', ' Amazon Simple Queue Service',
        ' Amazon Simple Storage Service	', ' Amazon Virtual Private Cloud', 'AmazonCloudWatch']

    short_services = ['AWS KMS', ' Amazon ECR', ' Amazon EC2', ' Amazon ECS', ' Amazon EKS', ' Amazon EFS', 
        ' Amazon ELB', ' Amazon Elasticsearch', ' Amazon RDS', ' Amazon SES', ' Amazon SNS', ' Amazon SQS', 
        ' Amazon S3', ' Amazon VPC', 'Amazon CloudWatch']

    df_services = pd.DataFrame ( {'Service': services, 
                        'ShortService': short_services })

    return df_services

def data_cleaning (df, df_services):
    # Rename columns from dataframe and replace tags 'refund'
    df.columns = [ 'Id', 'Customer', 'Email', 'Status', 'Date', 'Service', 'Amount', 'Unit' ]
    df.loc [df.Service == 'Refund', ['Id']] = '000 - ' + df['Customer']
    df.loc [df.Service == 'Refund', ['Customer', 'Email']] = 'Refund', ''

    # Removing both the leading and the trailing characters
    df['Service'], df_services['Service'] = df['Service'].str.strip(), df_services['Service'].str.strip()
    df['Service'].replace(df_services.set_index('Service').to_dict()['ShortService'],
                      inplace=True)
    return df

def saving_file (df):
    files_name, files_type = 'aws_' + df['Date'][0], ['csv', 'json']
    cs_type, df_type = ['text/csv', 'application/json'], [df.to_csv(), df.to_json()]

    storage_client = storage.Client()
    uri = 'bghtp-aws-billing-api-savings' 
    bucket = storage_client.get_bucket(uri)

    for i in [0, 1]:
        destination_blob_name = '{files_type}/{files_name}.{files_type}' .format (files_name = files_name, files_type = files_type[i])
        bucket.blob(destination_blob_name).upload_from_string(df_type[i], cs_type[i])

def main (event, context):
    '''
    Making an API call over AWS Organizations and getting accounts,
    Getting info for each billing account,
    Generating a pandas dataframe, 
    Appling data cleaning over the pandas dataframe
    '''
    df = pd.DataFrame.from_dict ( getBilling (getAccounts() )) 
    df = data_cleaning (df, short_services () )

    # Generating variables for csv and json files 
    try:
        saving_file (df)
    except Exception as e:
        print (e, 'error')

    # Loading pandas dataframe to bigquery table
    loading_bigquery(df)

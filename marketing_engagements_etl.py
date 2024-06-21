import sys
import pandas as pd
import boto3
import pytz
from datetime import datetime,timedelta,timezone,time,date
from dateutil.relativedelta import relativedelta
import requests
import snowflake.connector
from sqlalchemy import create_engine
import json
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_der_private_key

# AWS S3 Configuration
s3_bucket = 'aws-glue-assets-bianalytics'
s3_key = 'BIZ_OPS_ETL_USER.p8'

# Function to download file from S3
def download_from_s3(bucket, key):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        return response['Body'].read()
    except Exception as e:
        print(f"Error downloading from S3: {e}")
        return None

# Download the private key file from S3
key_data = download_from_s3(s3_bucket, s3_key)

def get_secrets(secret_names, region_name="us-east-1"):
    secrets = {}
    
    client = boto3.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    
    for secret_name in secret_names:
        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name)
        except Exception as e:
                raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secrets[secret_name] = get_secret_value_response['SecretString']
            else:
                secrets[secret_name] = base64.b64decode(get_secret_value_response['SecretBinary'])

    return secrets
    
def extract_secret_value(data):
    if isinstance(data, str):
        return json.loads(data)
    return data

secrets = ['hs_engagements_object_id','hs_etl_token','snowflake_account','snowflake_fivetran_db','snowflake_fivetran_wh','snowflake_bizops_wh','snowflake_bizops_role','snowflake_key_pass','snowflake_bizops_user']

fetch_secrets = get_secrets(secrets)

extracted_secrets = {key: extract_secret_value(value) for key, value in fetch_secrets.items()}

snowflake_user = extracted_secrets['snowflake_bizops_user']['snowflake_bizops_user']
snowflake_account = extracted_secrets['snowflake_account']['snowflake_account']
object_type_id = extracted_secrets['hs_engagements_object_id']['hs_engagements_object_id']
access_token = extracted_secrets['hs_etl_token']['hs_etl_token']
snowflake_fivetran_db = extracted_secrets['snowflake_fivetran_db']['snowflake_fivetran_db']
snowflake_bizops_wh = extracted_secrets['snowflake_bizops_wh']['snowflake_bizops_wh']
snowflake_role = extracted_secrets['snowflake_bizops_role']['snowflake_bizops_role']
snowflake_key_pass = extracted_secrets['snowflake_key_pass']['snowflake_key_pass']

password = snowflake_key_pass.encode()

# Try loading the private key as PEM
private_key = load_pem_private_key(key_data, password=password)

# Extract the private key bytes in PKCS8 format
private_key_bytes = private_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

headers = {
    'Authorization': f'Bearer {access_token}',
    'Content-Type': 'application/json'
}

#Store results from lists endpoint request
all_results = []
initial_url = 'https://api.hubapi.com/crm/v3/lists/9705/memberships'

next_url = initial_url

#Paginate through until payload is complete
while next_url:
    response = requests.get(next_url, headers=headers)
    
    if response.status_code == 200:
        data = response.json()
        #Append current batch of results
        all_results.extend(data.get('results', []))  

        #Check if there's a next page and update the next_url for the next request
        paging_info = data.get('paging', {}).get('next')
        if paging_info:
            #Use this link for the next request
            next_url = paging_info['link']  
        else:
            next_url = None
    else:
        #Error handling
        next_url = None

#Init snowflake session
ctx = snowflake.connector.connect(
    user=snowflake_user,
    account=snowflake_account,
    private_key=private_key_bytes,
    role=snowflake_role,
    warehouse=snowflake_bizops_wh)

#Fetch already existing marketing engagement object ids from data table
cs = ctx.cursor()
script = f"""
select * from "{snowflake_fivetran_db}"."HUBSPOT"."MARKETING_ENGAGEMENTS"
"""
payload = cs.execute(script)
current_df = pd.DataFrame.from_records(iter(payload), columns=[x[0] for x in payload.description])

#Store ids from data table in list
df_id_list = current_df['ID'].tolist()
#Compare ids from List request with IDs in table
df_list_integers = [int(x) for x in df_id_list]

#Store non-matches in list
non_matches = [int(x['recordId']) for x in all_results if int(x['recordId']) not in df_list_integers]

#Batch request applicable for only net new engagement ids
batch_read_url = f'https://api.hubapi.com/crm/v3/objects/{object_type_id}/batch/read'

properties = ["hs_createdate", "company_name", "email_address", "email_name", "engagement_date", "first_name", "last_name", "form_name",
    "hubspot_contact_record_id", "icapture_lead_rating", "lead_source___most_recent", "marketing_engagement_type",
    "partner_of_interest", "salesforce_account_id", "salesforce_campaign_id", "salesforce_campaign_name",
    "salesforce_contact_id", "salesforce_lead_id", "url","mql_activity","hubspot_score___activity_score",
    "hubspot_score___profile___activity","hubspot_score___profile_score","hubspot_score___updated","i_m_a___","contact_record_type","tax_id___contact","event_name"]

#Max batch size
batch_size = 100

detailed_data_list = []

#Chunk the non-matches list into smaller batches and send requests
for i in range(0, len(non_matches), batch_size):
    batch_ids = non_matches[i:i + batch_size]

    inputs = [{"id": id} for id in batch_ids]

    #JSON body to specify request properties and input parameters
    data = {
        "properties": properties,
        "inputs": inputs
    }

    #Send the request
    response = requests.post(batch_read_url, headers=headers, data=json.dumps(data))

    if response.status_code == 200:
        detailed_data = response.json()
        for item in detailed_data['results']:
            row = {'id': item['id']}
            row.update(item['properties'])
            detailed_data_list.append(row)
    else:
        break
        #Should only execute if error, error will log in logging file

#Store results from batch request in pandas dataframe
update_df = pd.DataFrame(detailed_data_list)

#Convert datetime fields to UTC, without manually specifying format
if update_df.empty:
    sys.exit(0)
    
update_df['hs_createdate'] = pd.to_datetime(update_df['hs_createdate'], errors='coerce', utc=True)
update_df['hs_lastmodifieddate'] = pd.to_datetime(update_df['hs_lastmodifieddate'], errors='coerce', utc=True)

#Define EST
est_timezone = pytz.timezone("US/Eastern")

#Convert UTC time to EST; datetime objects are already tz-aware due to `utc=True`
update_df['hs_createdate_est'] = update_df['hs_createdate'].dt.tz_convert(est_timezone)
update_df['hs_lastmodifieddate_est'] = update_df['hs_lastmodifieddate'].dt.tz_convert(est_timezone)

#Format the datetime objects as strings in EST, including only the timezone offset
update_df['hs_createdate_est_str'] = update_df['hs_createdate_est'].dt.strftime("%Y-%m-%d %H:%M:%S %z")
update_df['hs_lastmodifieddate_est_str'] = update_df['hs_lastmodifieddate_est'].dt.strftime("%Y-%m-%d %H:%M:%S %z")

#Do some minor data transformation
update_df.drop(['hs_createdate', 'hs_lastmodifieddate', 'hs_createdate_est_str', 'hs_lastmodifieddate_est_str','hs_object_id'], axis=1, inplace=True)
update_df.rename(columns={'hs_createdate_est': 'hs_createdate', 'hs_lastmodifieddate_est': 'hs_lastmodifieddate','hubspot_score___profile_score':'profile_score','i_m_a___':'im_a',
'hubspot_score___activity_score':'activity_score','hubspot_score___profile___activity':'profile_activity','hubspot_score___updated':'updated_score','tax_id___contact':'TAX_ID','event_name':'EVENT_NAME'}, inplace=True)

update_df.rename(columns={'hubspot_contact_record_id': 'hs_contact_id'}, inplace=True)
update_df.drop(columns='hs_lastmodifieddate', inplace=True)

update_df.columns = update_df.columns.str.upper()

connection_string = f"snowflake://{snowflake_user}@{snowflake_account}/{snowflake_fivetran_db}/HUBSPOT?warehouse={snowflake_bizops_wh}&role={snowflake_role}&authenticator=externalbrowser"

#Instantiate SQLalchemy engine
engine = create_engine(connection_string,connect_args={"private_key": private_key_bytes})

chunk_size = 10000
chunks = [x for x in range(0, len(update_df), chunk_size)] + [len(update_df)]
table_name = 'marketing_engagements' 

for i in range(len(chunks) - 1):
    print(f"Inserting rows {chunks[i]} to {chunks[i + 1]}")
    update_df[chunks[i]:chunks[i + 1]].to_sql(table_name, engine, if_exists='append', index=False)

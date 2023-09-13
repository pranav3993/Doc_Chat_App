import json
import pandas as pd
from pandas import json_normalize
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import os
import re

Connection_String_to_blob = os.environ.get('Connection_String_to_blob')

def get_blob_service_client():
    connection_string = Connection_String_to_blob
    return BlobServiceClient.from_connection_string(connection_string)

def upload_to_blob_storage(file_path, filename):
    blob_service_client = get_blob_service_client()
    container_name = 'peearzchatdocupload'  # Create a container in your Blob storage account

    # Get a blob client
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=filename)

    # Upload the file to Blob storage
    with open(file_path, 'rb') as data:
        blob_client.upload_blob(data, overwrite=True)

def json_csv(path_to_store, file):

    dictionary = json.loads(file)

    df = None
    first = True

    for document in dictionary:
        if document == None:
            continue
        else:
            dfLeft = json_normalize(document)

        for title, value in document.items():
            if type(value) == list:
                dfLeft.drop(title, axis="columns", inplace=True)
                dfRight = json_normalize(value)
                dfRight = dfRight.add_prefix(f"{title}_")
                dfLeft = pd.concat([dfLeft, dfRight], axis = 1)    

        if first:
            df = dfLeft
            first = False
        else:
            df = pd.concat([df, dfLeft], axis = 0)

    df.reset_index(inplace=True, drop=True)

    df1 = df.T.drop_duplicates().T

    #file_location = os.path.join(app.config['UPLOAD_FOLDER'])
    #df1.to_csv('file_path/test.csv')

    df1.to_csv('test.csv')
    #upload_to_blob_storage(os.path.join(path_to_store, 'test.csv'), 'test.csv')

def rename_ivalidfilename(filename):
    # Funtion used to Remove any characters that are non alphanumeric
    return re.sub(r'[^a-zA-Z0-9\.\-_]', '', filename)

def blob_filereader(account_name, account_key, container_name):
    # Funtion used to read all the files in blob storage
    try:
        # Creating BlobServiceClient by providing account credentials
        account_creds = BlobServiceClient(account_url=f"https://{account_name}.blob.core.windows.net", credential=account_key)
        
        # Getting a reference to the container
        container_client = account_creds.get_container_client(container_name)

        # Listing all files in the container
        blob_files = container_client.list_blobs()

        for blob in blob_files:
            # Extract the blob name
            blob_name = blob.name

            # Renaming the blob name to remove non alpanumeric characters
            rename_blob_name = rename_ivalidfilename(blob_name)

            # Downloading and handling files without attempting to decode them
            file_extension = rename_blob_name.split('.')[-1]
            blob_client = container_client.get_blob_client(blob_name)

            if file_extension in ['pdf', 'png', 'jpg', 'csv', 'json', 'txt']:
                try:
                    content = blob_client.download_blob().readall()
                    with open(rename_blob_name, 'wb') as file:
                        file.write(content)
                    print(f"File Downloaded: {rename_blob_name}")
                except Exception as e:
                    print(f"Error downloading {rename_blob_name}: {str(e)}")
            else:
                # Read and print the contents of text files
                try:
                    content = blob_client.download_blob().readall()
                    print(f"Contents of {rename_blob_name}: {content.decode('utf-8')}")
                except Exception as e:
                    print(f"Error reading {rename_blob_name}: {str(e)}")
    except Exception as e:
        print(f"An error occurred: {str(e)}")
        
        
account_name = 'peearztest'
account_key = '2SIi75BD/kTMdGkvCjH8uI1hTH436whrLkTKRJBOG413rV3VRGElBbtAg4zw9E0wn180jku8je5g+ASt42Kttw=='
container_name = 'peearziois'
blob_filereader(account_name, account_key, container_name)

'''def json_csv(path_to_csv, file):  # NEW FUNCTION 3
    #print("FILE IS :",file)
    df = pd.read_json(file)
    print(df.head())
    df.to_csv('test.csv')'''

'''
def fetch_file_from_blob(container_name, blob_name, destination_path):
    blob_service_client = get_blob_service_client()
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(blob_name)

    with open(destination_path, "wb") as file:
        blob_data = blob_client.download_blob()
        file.write(blob_data.readall())

def json_csv(json_file_path, csv_file_path):  NEW FUNCTION 2
    """
    Convert a JSON file to a CSV file.

    Parameters:
        json_file_path (str): Path to the input JSON file.
        csv_file_path (str): Path to the output CSV file.
    """
    with open(json_file_path, 'r') as json_file:
        data = json.load(json_file)

    with open(csv_file_path, 'w', newline='') as csv_file:
        # Extract the headers from the first item in the JSON data
        headers = list(data[0].keys())

        # Create a CSV writer and write the header row
        writer = csv.DictWriter(csv_file, fieldnames=headers)
        writer.writeheader()

        # Write each row to the CSV file
        writer.writerows(data)

def download_blob_to_file(container_name='peearzchatdocupload'):
    blob_service_client = get_blob_service_client()
    blob_client = blob_service_client.get_blob_client(container=container_name, blob="test.csv")
    with open(file=os.path.join(r'/Users/ankitanand/Documents/Peearz/', 'test.csv'), mode="wb") as sample_blob:
        download_stream = blob_client.download_blob()
        sample_blob =  download_stream.readall()
        return sample_blob'''

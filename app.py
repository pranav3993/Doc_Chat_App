import os
import sys
from tempfile import NamedTemporaryFile
from flask import Flask, request, render_template
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from langchain.llms import OpenAI
from langchain.agents import create_csv_agent
from dotenv import load_dotenv
from werkzeug.utils import secure_filename
from utils import json_csv, get_blob_service_client, upload_to_blob_storage
from exception import CustomException, querys
#import const

load_dotenv()

app=Flask(__name__)

openaiservicename = 'ms-openai-cosmos'
os.environ["OPENAI_API_TYPE"] = "azure"
os.environ["OPENAI_API_VERSION"] = '2023-05-15'
os.environ["OPENAI_API_BASE"] = f"https://{openaiservicename}.openai.azure.com"
os.environ["OPENAI_API_KEY"] = os.environ.get('OPENAI_API_KEY')

UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'file_path/')
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

@app.route('/')
def index():
    return render_template('home.html')

@app.route('/predictdata', methods=['GET','POST'])
def predict_datapoint():
    if request.method=='GET':
        return render_template('home.html')
    else:
        if 'file' not in request.files:
            return "No File Part"
    
        user_file = request.files['file']
        
        if user_file:
                
            filename = secure_filename(user_file.filename)

            temp_file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            user_file.save(temp_file_path)

            # Upload the file to Azure Blob storage
            upload_to_blob_storage(temp_file_path, filename)

            if filename.endswith(".csv"):
                #user_file.save(os.path.join(app.config['blob_path'], filename))      
    
                with NamedTemporaryFile() as f: # Create temporary file
                    f.write(user_file.getvalue()) # Save uploaded contents to file
                    f.flush()
                    llm=OpenAI(engine='text-davinci-002', temperature=0)
                    user_question = request.form['query']
                    agent=create_csv_agent(llm, f.name, verbose=True,  early_stopping_method="generate")

                if user_question is not None and user_question != "":
                    response = agent.run(user_question)

            elif filename.endswith(".json"):
                json_csv(UPLOAD_FOLDER, user_file.getvalue().decode('utf-8')) #converting the json file to csv for the csv agent

                #user_file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))  
                #json_content = user_file.read() #NEW FUNCTION 2 -> pass in json_content directly

                #print("***********************DEBUG***************************************")
                #print(UPLOAD_FOLDER+filename)
                #print("***********************DEBUG***************************************")
                
                #json_csv(UPLOAD_FOLDER, UPLOAD_FOLDER+filename) # NEW FUCNTION 3


                llm=OpenAI(engine='text-davinci-002', temperature=0)
                user_question = request.form['query']
                #file = os.path.join(app.config['UPLOAD_FOLDER'], filename)
                agent=create_csv_agent(llm, 'test.csv', verbose=True, early_stopping_method="generate")

                if user_question is not None and user_question != "":
                    response = agent.run(user_question)

            # Remove the temporary file after uploading
            os.remove(temp_file_path)

    return response
    
if __name__=="__main__":
    app.run(host="0.0.0.0", port="4999",debug=True)
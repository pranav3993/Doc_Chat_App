import sys
from langchain.llms import OpenAI
import openai

def querys(agent, query):
        try:
            #Make your OpenAI API request here
            response = agent.run(query)
        except openai.error.InvalidRequestError as e:
            # print(f"OpenAI API returned an Invalid Request Error: {e}")
            response = "You are getting this error as the question you ask requires greater capacity than the capacity of this model. Kindly rephase the question and try again. If it still does not work then the data provided is too large for this app to handle."
            pass
        except openai.error.AuthenticationError as e:
            #Handle connection error here
            # print(f"Authentication error: {e}")
            response = "You are getting this error because the API key that Peearz provided is invalid. Kindly notify us at abc@peearz.com to make the necessary fix."
            pass
        except openai.error.APIError as e:
            #Handle API error here, e.g. retry or log
            # print(f"OpenAI API returned an API Error: {e}")
            response = "API Error. Please reach out to the OpenAI team."
            pass
        except openai.error.RateLimitError as e:
            #Handle rate limit error (we recommend using exponential backoff)
            # print(f"OpenAI API request exceeded rate limit: {e}")
            response = "You have exceeded the rate limit applied to this OpenAI account."
            pass
        except:
            response = "unknown error - need to debug!"

        return response

def error_message_detail(error,error_detail:sys):
    _,_,exc_tb=error_detail.exc_info()
    file_name=exc_tb.tb_frame.f_code.co_filename
    error_message="Error occured in python script name [{0}] line number [{1}] error message[{2}]".format(
     file_name,exc_tb.tb_lineno,str(error))

    return error_message

class CustomException(Exception):
    def __init__(self,error_message,error_detail:sys):
        super().__init__(error_message)
        self.error_message=error_message_detail(error_message,error_detail=error_detail)
    
    def __str__(self):
        return self.error_message
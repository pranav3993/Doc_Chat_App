import requests
from flask import Flask, jsonify, request

app = Flask(__name__)

FHIR_BASE_URL = "launch.html?iss=https://fhir.epic.com/interconnect-fhir-oauth/api/FHIR/R4&aud="  # Replace with your FHIR server URL


@app.route("/patient/<patient_id>", methods=["GET"])
def get_patient_data(patient_id):
    # Example endpoint: /patient/12345

    # Build the FHIR URL to retrieve patient data
    fhir_url = f"{FHIR_BASE_URL}/Patient/{patient_id}"
    headers = {"Accept": "application/json"}

    try:
        # Send the GET request to the FHIR server
        response = requests.get(fhir_url, headers=headers)

        # Check if the response was successful (status code 200)
        if response.status_code == 200:
            # Return the FHIR data in JSON format
            return jsonify(response.json())
        else:
            # Return an error message if the request was not successful
            return jsonify({"error": "Failed to retrieve patient data"}), response.status_code

    except requests.RequestException as e:
        # Return an error message if there was a connection error
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=4999)  # Run the Flask app on localhost:5000

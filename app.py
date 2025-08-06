import os
import json
import pandas as pd
import xml.etree.ElementTree as ET
import re
import requests
import time
import io
import yaml
from flask import Flask, render_template, request, flash, redirect, url_for
from werkzeug.utils import secure_filename

# --- Configuration ---
# Reading the key from your local key.yaml file
with open("key.yaml", "r") as f:
    config = yaml.safe_load(f)
GEMINI_API_KEY = config.get("GEMINI_API_KEY")

# Using the correct, powerful model for this task
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key={GEMINI_API_KEY}"

# Using local folders for testing
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
PROCESSED_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'processed')
ALLOWED_EXTENSIONS = {'txt', 'log', 'json', 'xml', 'csv'}

# The canonical, final schema for all logs
GENERIC_SCHEMA = [
    'timestamp', 'log_level', 'message', 'service_name', 
    'host_name', 'trace_id', 'error_details', 'metadata'
]

# --- App Initialization ---
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER
app.config['SECRET_KEY'] = 'a-very-secret-key-for-local-testing'

# --- Helper Functions ---

def allowed_file(filename):
    """Checks if the file's extension is in the ALLOWED_EXTENSIONS set."""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def get_regex_from_gemini(log_sample):
    """
    Sends a sample of the log file to the Gemini API to determine a parsing regex.
    """
    if not GEMINI_API_KEY:
        raise ValueError("GEMINI_API_KEY not found in key.yaml.")

    # --- UPDATED PROMPT with stricter message definition ---
    prompt = f"""
    You are an expert log data transformation engine. Your task is to analyze a raw log sample and generate a Python-compatible regular expression (regex) that extracts key information and maps it to a predefined generic schema.

    **Generic Log Schema:**
    - `timestamp`: The full timestamp of the event.
    - `log_level`: The severity of the event (e.g., INFO, WARN, ERROR).
    - `message`: The primary, human-readable message.
    - `service_name`: The application or service that generated the log.
    - `host_name`: The hostname of the machine.
    - `trace_id`: A unique identifier for correlating logs.
    - `error_details`: Stack trace or detailed error messages.
    - `metadata`: A catch-all for any other structured data.

    **Log Sample to Analyze:**
    ---
    {log_sample}
    ---

    **Your Instructions:**
    1.  Create a single Python regex with named capture groups (e.g., `?P<group_name>...`).
    2.  The name of each capture group **MUST** match a key from the **Generic Log Schema** if possible. Use the following mapping rules:
        - For a unique identifier (like a UUID, `correlation_id`, `request_id`, `tid`, `euid`), name the capture group `trace_id`.
        - For an exception or stack trace (like `exception_details`, `stack_trace`), name the capture group `error_details`.
        - For a service or application name, use `service_name`.
        - For a server or machine name, use `host_name`.
    3.  If a field from the log does not logically map to any standard schema key, create a capture group with a descriptive, snake_case name (e.g., `machine_id`, `user_id`). The Python code will handle putting these into the `metadata` field.
    4.  **CRITICAL RULE:** The `timestamp` group must capture the entire date and time component as one field.
    5.  **CRITICAL RULE for `message`:** The `message` is the human-readable text that typically follows the timestamp and log level but comes BEFORE any other structured key-value pairs or identifiers.
    6.  The regex should match the entire line from start (`^`) to end (`$`).
    7.  **Return ONLY the raw regex string and absolutely nothing else.**

    **Example 1: For a complex, pipe-delimited log like "2025-08-05 17:15:21 | WARNING | Profile updated | email-dispatcher | host-02 | a870-d0a6 | TimeoutException | ...":**
    ^(?P<timestamp>.*?)\\s*\\|\\s*(?P<log_level>\\w+)\\s*\\|\\s*(?P<message>.*?)\\s*\\|\\s*(?P<service_name>.*?)\\s*\\|\\s*(?P<host_name>.*?)\\s*\\|\\s*(?P<trace_id>.*?)\\s*\\|\\s*(?P<error_details>.*?)\\s*\\|\\s*(?P<json_payload>{{.*}})$
    
    **Example 2: For a standard log like "[Sun Dec 04 04:51:18 2005] [error] mod_jk child workerEnv in error state 6":**
    Your output should be a regex like:
    ^\\[(?P<timestamp>.*?)\\] \\[(?P<log_level>\\w+)\\] (?P<message>.*)$
    """    
    payload = {"contents": [{"parts": [{"text": prompt}]}]}
    headers = {'Content-Type': 'application/json'}
    
    retries = 3
    backoff_factor = 2
    for i in range(retries):
        try:
            response = requests.post(GEMINI_API_URL, headers=headers, json=payload, timeout=90)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if e.response is not None and e.response.status_code in [503, 429] and i < retries - 1:
                time.sleep(backoff_factor)
                backoff_factor *= 2
                continue
            else:
                raise e
    
    raise requests.exceptions.RequestException("Failed to get a response from the API after several retries.")

# --- Flask Routes ---

@app.route('/', methods=['GET'])
def index():
    """Renders the main upload page."""
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles the file upload and processing logic."""
    if 'file' not in request.files:
        flash('No file part')
        return redirect(url_for('index'))
    
    file = request.files['file']

    if file.filename == '':
        flash('No selected file')
        return redirect(url_for('index'))

    if file and allowed_file(file.filename):
        try:
            filename = secure_filename(file.filename)
            upload_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            file.save(upload_path)
            
            with open(upload_path, 'r', encoding='utf-8', errors='ignore') as f:
                file_content = f.read()

            if not file_content:
                flash("The uploaded file appears to be empty.")
                return redirect(url_for('index'))

            ext = filename.rsplit('.', 1)[1].lower()
            df = None
            
            if ext in ['log', 'txt']:
                log_sample = "\n".join(file_content.splitlines()[:10])
                
                if not log_sample:
                    flash("File is empty.")
                    return redirect(url_for('index'))

                gemini_response = get_regex_from_gemini(log_sample)
                regex_pattern = gemini_response.get('candidates', [{}])[0].get('content', {}).get('parts', [{}])[0].get('text')

                if not regex_pattern:
                    raise ValueError("LLM did not return a regex pattern.")
                
                if regex_pattern.strip().startswith("```"):
                    regex_pattern = re.sub(r'```(python)?\n', '', regex_pattern)
                    regex_pattern = regex_pattern.strip().replace('```', '')

                print("\n--- Generated Regex Pattern ---")
                print(regex_pattern.strip())
                print("-----------------------------\n")

                pattern = re.compile(regex_pattern.strip())
                parsed_data = [match.groupdict() for line in file_content.splitlines() if (match := pattern.match(line.strip()))]
                df = pd.DataFrame(parsed_data)

                if df.empty:
                    print("AI regex parsing failed. Using fallback method.")
                    flash("AI parsing could not find a pattern. Displaying raw lines instead.")
                    parsed_data = [{'message': line} for line in file_content.splitlines()]
                    df = pd.DataFrame(parsed_data)

            elif ext == 'json':
                df = pd.read_json(upload_path)

            elif ext == 'xml':
                tree = ET.parse(upload_path)
                records = [{child.tag: child.text for child in elem} for elem in tree.getroot()]
                df = pd.DataFrame(records)
            
            elif ext == 'csv':
                df = pd.read_csv(upload_path)

            if df is None or df.empty:
                flash('Parsing failed. The application could not structure the data.')
                return redirect(url_for('index'))

            metadata_cols = [col for col in df.columns if col not in GENERIC_SCHEMA]
            
            if metadata_cols:
                df['metadata'] = df[metadata_cols].apply(
                    lambda row: row.to_json(), axis=1
                )
                df = df.drop(columns=metadata_cols)

            df = df.reindex(columns=GENERIC_SCHEMA)

            csv_string = df.to_csv(index=False)
            
            csv_filename = f"{filename.rsplit('.', 1)[0]}_processed.csv"
            
            preview_headers = df.columns.values.tolist()
            rows = df.head(100).fillna('').values.tolist()
            
            return render_template('results.html', 
                                   csv_filename=csv_filename,
                                   headers=preview_headers,
                                   rows=rows,
                                   full_csv_data=csv_string)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            flash(f'An error occurred: {e}')
            return redirect(url_for('index'))

    else:
        flash('File type not allowed.')
        return redirect(url_for('index'))

if __name__ == '__main__':
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(PROCESSED_FOLDER, exist_ok=True)
    app.run(debug=True)

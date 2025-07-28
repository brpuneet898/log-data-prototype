import os
import json
import pandas as pd
import xml.etree.ElementTree as ET
import re
from flask import Flask, render_template, request, send_from_directory, flash, redirect, url_for
from werkzeug.utils import secure_filename

# --- Configuration ---
# Define the paths for file uploads and processed files.
# It's good practice to use absolute paths.
UPLOAD_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'uploads')
PROCESSED_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'processed')
ALLOWED_EXTENSIONS = {'txt', 'log', 'json', 'xml'}

# --- App Initialization ---
app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['PROCESSED_FOLDER'] = PROCESSED_FOLDER
app.config['SECRET_KEY'] = 'supersecretkey' # Change this in a real application

# --- Helper Functions ---

def allowed_file(filename):
    """Checks if the file's extension is in the ALLOWED_EXTENSIONS set."""
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def parse_log_file(file_path):
    """
    A generic log parser. This is a basic example.
    Real-world log files can be very complex. This function tries to extract
    common patterns like IP addresses, timestamps, and request methods.
    You will likely need to customize this regex for your specific log formats.
    """
    # Example Regex: Captures IP, timestamp, request method/path, status, and size.
    # This is a common pattern for web server logs (e.g., Apache, Nginx).
    log_pattern = re.compile(r'(?P<ip>\S+) \S+ \S+ \[(?P<timestamp>.*?)\] "(?P<request>.*?)" (?P<status>\d{3}) (?P<size>\S+)')
    data = []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            match = log_pattern.match(line)
            if match:
                data.append(match.groupdict())
    if not data:
        # Fallback for unstructured logs: treat each line as a single message.
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            data = [{'message': line.strip()} for line in f]
    return pd.DataFrame(data)

def parse_json_file(file_path):
    """
    Parses a JSON file. Handles both standard JSON and line-delimited JSON (JSONL).
    """
    try:
        # Try parsing as a standard JSON array of objects
        df = pd.read_json(file_path)
    except ValueError:
        # If that fails, try parsing as line-delimited JSON
        with open(file_path, 'r') as f:
            data = [json.loads(line) for line in f]
        df = pd.DataFrame(data)
    return df

def parse_xml_file(file_path):
    """
    Parses an XML file. Assumes a structure where the root has many
    child elements, and each child is a record.
    """
    tree = ET.parse(file_path)
    root = tree.getroot()
    data = []
    for elem in root:
        record = {child.tag: child.text for child in elem}
        data.append(record)
    return pd.DataFrame(data)

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
        return redirect(request.url)
    
    file = request.files['file']

    if file.filename == '':
        flash('No selected file')
        return redirect(request.url)

    if file and allowed_file(file.filename):
        filename = secure_filename(file.filename)
        upload_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
        file.save(upload_path)

        try:
            # Determine file type and parse accordingly
            ext = filename.rsplit('.', 1)[1].lower()
            df = None
            if ext in ['log', 'txt']:
                df = parse_log_file(upload_path)
            elif ext == 'json':
                df = parse_json_file(upload_path)
            elif ext == 'xml':
                df = parse_xml_file(upload_path)

            if df is None or df.empty:
                flash('Could not parse the file. The format might be unsupported or the file is empty.')
                return redirect(url_for('index'))

            # Create the output CSV
            base_filename = filename.rsplit('.', 1)[0]
            csv_filename = f"{base_filename}.csv"
            csv_path = os.path.join(app.config['PROCESSED_FOLDER'], csv_filename)
            df.to_csv(csv_path, index=False)
            
            # Redirect to the results page
            return render_template('results.html', csv_filename=csv_filename)

        except Exception as e:
            flash(f'An error occurred while processing the file: {e}')
            return redirect(url_for('index'))

    else:
        flash('File type not allowed.')
        return redirect(request.url)

@app.route('/download/<filename>')
def download_file(filename):
    """Serves the processed CSV file for download."""
    return send_from_directory(app.config['PROCESSED_FOLDER'], filename, as_attachment=True)


# --- Main Execution ---
if __name__ == '__main__':
    # Create upload and processed directories if they don't exist
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)
    os.makedirs(PROCESSED_FOLDER, exist_ok=True)
    app.run(debug=True)

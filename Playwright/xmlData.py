import json
import os
from datetime import datetime
from dicttoxml import dicttoxml
import time

def process_data():
    # Read the last processed unique ID
    last_processed_id = ''
    if os.path.exists('last_processed.txt'):
        with open('last_processed.txt', 'r') as f:
            last_processed_id = f.read().strip()

    # Read new data from the JSON file
    new_data = []
    with open('processed_job_data.json', 'r') as f:
        for line in f:
            job = json.loads(line)
            if job['uniqueId'] > last_processed_id:
                new_data.append(job)

    if not new_data:
        print("No new data to process.")
        return

    # Update the last processed ID
    with open('last_processed.txt', 'w') as f:
        f.write(new_data[-1]['uniqueId'])

    # Process new data
    for job in new_data:
        state = job['location']['state']
        today = datetime.now().strftime('%d%m%Y')
        filename = f"{state}_{today}.xml"
        filepath = os.path.join('xmlData', filename)

        # Create xmlData directory if it doesn't exist
        os.makedirs('xmlData', exist_ok=True)

        # Check if the file exists and if the job is already in it
        job_exists = False
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                content = f.read()
                if job['uniqueId'] in content:
                    job_exists = True

        if not job_exists:
            # Append or create the XML file
            mode = 'a' if os.path.exists(filepath) else 'w'
            with open(filepath, mode) as f:
                if mode == 'w':
                    f.write('<?xml version="1.0" encoding="UTF-8"?>\n<jobs>\n')
                xml_data = dicttoxml({'job': job}, root=False, attr_type=False)
                f.write(xml_data.decode() + '\n')
                if mode == 'w':
                    f.write('</jobs>')

def main():
    while True:
        process_data()
        time.sleep(600)  # Sleep for 10 minutes

if __name__ == "__main__":
    main()
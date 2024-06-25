import json
import os
from datetime import datetime
from dicttoxml import dicttoxml
import shutil 
import time
import json

def update_last_processed_times(filename, timestamp):
    try:
        with open('last_processed_times.json', 'r', encoding='utf-8') as f:
            times = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        times = {}
    
    times[filename] = timestamp.isoformat()
    
    with open('last_processed_times.json', 'w', encoding='utf-8') as f:
        json.dump(times, f)

def get_last_processed_times():
    try:
        with open('last_processed_times.json', 'r', encoding='utf-8') as f:
            return {k: datetime.fromisoformat(v) for k, v in json.load(f).items()}
    except (FileNotFoundError, json.JSONDecodeError):
        return {}

def remove_processed_time(filename):
    times = get_last_processed_times()
    if filename in times:
        del times[filename]
        with open('last_processed_times.json', 'w', encoding='utf-8') as f:
            json.dump({k: v.isoformat() for k, v in times.items()}, f)

def extract_id_number(id_str) :
    return int(id_str.split('_')[0])

def get_current_batch():
    if os.path.exists('batch_record.txt'):
        with open('batch_record.txt', 'r') as f:
            lines = f.readlines()
            return lines[-1].strip()
    return "batch1"

def move_old_xml_files(old_batch):
    alldata_dir = os.path.join('xmlData', 'alldata')
    os.makedirs(alldata_dir, exist_ok=True)
    
    batch_dir = os.path.join(alldata_dir, old_batch)
    os.makedirs(batch_dir, exist_ok=True)
    
    last_processed_times = get_last_processed_times()
    
    for filename in os.listdir('xmlData'):
        file_path = os.path.join('xmlData', filename)
        if os.path.isfile(file_path) and filename.endswith('.xml'):
            last_processed_time = last_processed_times.get(filename)
            if last_processed_time:
                file_mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
                if file_mod_time <= last_processed_time:
                    shutil.move(file_path, os.path.join(batch_dir, filename))
                    remove_processed_time(filename)
                    print(f"Moved {filename} to {old_batch}")

def process_data():
    
    # Read the last processed unique ID
    last_processed_id = ''
    if os.path.exists('last_processed.txt'):
        with open('last_processed.txt', 'r') as f:
            last_processed_id = f.read().strip()
    last_processed_num = extract_id_number(last_processed_id) if last_processed_id else -1

    # Read new data from the JSON file
    new_data = []
    with open('raw_job_data.json', 'r') as f:
        
        for line in f:
            job = json.loads(line)
            if extract_id_number(job['unique_id']) > last_processed_num:
                new_data.append(job)
        print("Found new data to process")

    if not new_data:
        print("No new data to proces.")
        return

    # Update the last processed ID
    with open('last_processed.txt', 'w') as f:
        f.write(new_data[-1]['unique_id'])

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
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                if job['unique_id'] in content:
                    job_exists = True

        if not job_exists:
            # Append or create the XML file
            mode = 'a' if os.path.exists(filepath) else 'w'
            with open(filepath, mode, encoding='utf-8') as f:
                # if mode == 'w':
                    # f.write('<?xml version="1.0" encoding="UTF-8"?>\n<jobs>\n')
                xml_data = dicttoxml({'job': job}, root=False, attr_type=False)
                xml_data = xml_data.decode('utf-8').replace('\u200b', '').encode('utf-8')
                f.write(xml_data.decode('utf-8') + '\n')
                # if mode == 'w':
                #     f.write('</jobs>')
            update_last_processed_times(filename, datetime.now())
    with open('last_processed_timestamp.txt', 'w') as f:
        f.write(datetime.now().isoformat())

def main():
    last_checked_batch = get_current_batch()

    while True:
        process_data()
        current_batch = get_current_batch()
        move_old_xml_files(last_checked_batch)
        
        if current_batch != last_checked_batch:
            last_checked_batch = current_batch

        time.sleep(1800)

if __name__ == "__main__":
    main()
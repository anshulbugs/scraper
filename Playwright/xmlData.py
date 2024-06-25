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
    
    if filename in times:
        times[filename] = {
            'last': timestamp.isoformat(),
            'second_last': times[filename]['last']
        }
    else:
        times[filename] = {
            'last': timestamp.isoformat(),
            'second_last': None
        }
    
    with open('last_processed_times.json', 'w', encoding='utf-8') as f:
        json.dump(times, f)

def get_last_processed_times():
    try:
        with open('last_processed_times.json', 'r', encoding='utf-8') as f:
            times = json.load(f)
            return {k: {'last': datetime.fromisoformat(v['last']),
                        'second_last': datetime.fromisoformat(v['second_last']) if v['second_last'] else None}
                    for k, v in times.items()}
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

def move_old_xml_files(filename,old_batch):
    alldata_dir = os.path.join('xmlData', 'alldata')
    os.makedirs(alldata_dir, exist_ok=True)
    
    batch_dir = os.path.join(alldata_dir, old_batch)
    os.makedirs(batch_dir, exist_ok=True)
    file_path = os.path.join('xmlData', filename)
    if os.path.isfile(file_path):
        shutil.move(file_path, os.path.join(batch_dir, filename))
        print(f"Moved {filename} to {old_batch}")

def process_data(old_batch):
    
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
    modified_files = set()

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
            modified_files.add(filepath)
        # Move files that were not modified
    for filename in os.listdir('xmlData'):
        filepath = os.path.join('xmlData', filename)
        if os.path.isfile(filepath) and filepath not in modified_files:
            move_old_xml_files(filename,old_batch)

def main():
    last_checked_batch = get_current_batch()

    while True:
        process_data(last_checked_batch)
        current_batch = get_current_batch()
        # move_old_xml_files(last_checked_batch)
        
        if current_batch != last_checked_batch:
            last_checked_batch = current_batch

        time.sleep(1800)

if __name__ == "__main__":
    main()
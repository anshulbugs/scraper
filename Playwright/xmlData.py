import json
import os
from datetime import datetime
from dicttoxml import dicttoxml
import shutil 
import time
import json
import glob

def get_current_batch():
    if os.path.exists('batch_record.txt'):
        with open('batch_record.txt', 'r') as f:
            lines = f.readlines()
            return lines[-1].strip()
    return "batch1"

def move_old_xml_files(filename, old_batch):
    alldata_dir = os.path.join('xmlData', 'alldata')
    os.makedirs(alldata_dir, exist_ok=True)
    
    batch_dir = os.path.join(alldata_dir, old_batch)
    os.makedirs(batch_dir, exist_ok=True)
    file_path = os.path.join('xmlData', filename)
    
    if os.path.isfile(file_path):
        temp_file = file_path + '.temp'
        
        # Write the prepend content to the temporary file
        with open(temp_file, 'wb') as temp:
            temp.write(b'<?xml version="1.0" encoding="UTF-8"?>\n<jobs>\n')
        
        # Copy the original file content in chunks
        with open(file_path, 'rb') as original, open(temp_file, 'ab') as temp:
            shutil.copyfileobj(original, temp, length=1024*1024)  # 1MB chunks
        
        # Append the closing content
        with open(temp_file, 'ab') as temp:
            temp.write(b'</jobs>')
        
        # Replace the original file with the new file
        os.replace(temp_file, file_path)

        # Move the modified file
        destination_path = os.path.join(batch_dir, filename)
        shutil.move(file_path, destination_path)
        print(f"Modified and moved {filename} to {old_batch}")

def process_data(old_batch, chunk_size=5000):
    # Read the last processed unique ID
    last_processed_id = -1
    if os.path.exists('last_processed.txt'):
        with open('last_processed.txt', 'r') as f:
            last_processed_id = f.read().strip()
    last_processed_num = int(last_processed_id) if last_processed_id else -1

    # Create xmlData directory if it doesn't exist
    os.makedirs('xmlData', exist_ok=True)

    modified_files = set()
    processed_count = 0
    max_id = last_processed_num

    # Process data in chunks
    with open('raw_job_data.json', 'r') as f:
        # Skip lines up to last_processed_num
        for _ in range(last_processed_num + 1):
            next(f, None)
        while True:
            chunk = []
            for _ in range(chunk_size):
                line = f.readline()
                if not line:
                    break
                job = json.loads(line)
                if job['id'] > last_processed_num:
                    chunk.append(job)
                    max_id = max(max_id, job['id'])

            if not chunk:
                break

            process_chunk(chunk, modified_files)
            processed_count += len(chunk)
            print(f"Processed {processed_count} new records")

    if processed_count == 0:
        print("No new data to process.")
        if os.path.exists('xmlData'):
            for filename in os.listdir('xmlData'):
                filepath = os.path.join('xmlData', filename)
                move_old_xml_files(filename, old_batch)
        return

    # Update last processed ID
    with open('last_processed.txt', 'w') as f:
        f.write(str(max_id))

    # Move files that were not modified
    for filename in os.listdir('xmlData'):
        filepath = os.path.join('xmlData', filename)
        if os.path.isfile(filepath) and filepath not in modified_files:
            move_old_xml_files(filename, old_batch)

def process_chunk(chunk, modified_files):
    for job in chunk:
        state = job['location']['state']
        #today = datetime.now().strftime('%d%m%Y')
        state = state.replace(' ', '_')
        existing_files = glob.glob(os.path.join('xmlData', f"{state}_*.xml"))
        
        if existing_files:
            # Use the most recent existing file
            filepath = max(existing_files, key=os.path.getmtime)
        else:
            # If no existing file, create a new one with today's date
            today = datetime.now().strftime('%d%m%Y')
            filename = f"{state}_{today}.xml"
            filepath = os.path.join('xmlData', filename)

        # Check if the job is already in the file
        job_exists = False
        #if os.path.exists(filepath):
            #with open(filepath, 'r', encoding='utf-8') as f:
                #content = f.read()
                #if job['unique_id'] in content:
                    #job_exists = True

        if not job_exists:
            # Append or create the XML file
            mode = 'a' if os.path.exists(filepath) else 'w'
            with open(filepath, mode, encoding='utf-8') as f:
                xml_data = dicttoxml({'job': job}, root=False, attr_type=False)
                xml_data = xml_data.decode('utf-8').replace('\u200b', '').encode('utf-8')
                f.write(xml_data.decode('utf-8') + '\n')
            modified_files.add(filepath)
def main():
    last_checked_batch = get_current_batch()

    while True:
        process_data(last_checked_batch)
        current_batch = get_current_batch()
        # move_old_xml_files(last_checked_batch)
        
        if current_batch != last_checked_batch:
            last_checked_batch = current_batch

        time.sleep(300)

if __name__ == "__main__":
    main()

import asyncio
import psutil
from playwright.async_api import async_playwright, Playwright
import logging
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import json
from dicttoxml import dicttoxml
import replicate
import os
from json_repair import repair_json
import re
# Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
MAX_REQUESTS_PER_MINUTE = 600
RATE_LIMIT = int(MAX_REQUESTS_PER_MINUTE / 60)
id_count = 1
def write_to_file(data, filename, file_format='json'):
    try:
        with open(filename, 'a') as f:
            if file_format == 'json':
                for item in data:
                    json.dump(item, f)
                    f.write('\n')  # Add a newline character after each JSON object
            elif file_format == 'xml':
                f.write('<jobs>\n')
                for item in data:
                    xml_data = dicttoxml({'job': item}, root=False, attr_type=False)
                    f.write(xml_data.decode() + '\n')
                f.write('</jobs>')
    except Exception as e:
        # logger.error(f"Error writing to file {filename}: {e}")
        print("Error writing to file",e)

def clean_state_name(state):
    # Extract the first two letters from the state name
    return state[:2]

def clean_unique_id(unique_id):
    # Remove any special characters, commas, and spaces but keep underscores from uniqueId
    return re.sub(r'[^A-Za-z0-9_]', '', unique_id)

def clean_location(location):
    # Remove any text within parentheses in the location
    return re.sub(r'\s*\([^)]*\)', '', location)

def get_latitude_longitude(city, state, location_data):
    # Search for the city and state in the location data to get latitude and longitude
    for location in location_data:
        if location['name'].lower() == city.lower() and location['state_code'].upper() == state.upper():
            return location['latitude'], location['longitude']
    return None, None


async def extract_job_description_with_openai_async(data, semaphore, max_retries=3, retry_delay=2):
    async with semaphore:
        return await retry_async(
            extract_job_description_with_openai, 
            data, max_retries=max_retries, retry_delay=retry_delay
        )

async def retry_async(func, *args, max_retries=2, retry_delay=1):
    attempt = 0
    while attempt < max_retries:
        try:
            return await func(*args)
        except Exception as e:
            # logger.error(f"Error on attempt {attempt + 1}: {e}")
            print(f"Error on attempt {attempt + 1}: {e}")
            attempt += 1
            if attempt < max_retries:
                await asyncio.sleep(retry_delay * (2 ** attempt))
            else:
                # logger.error("Max retries reached. Returning fallback response.")
                print("Max retries reached. Returning fallback response.")
                return None

    
async def extract_job_description_with_openai(data):
    job_description = data['position_description']
    location = data['location']
    os.environ["REPLICATE_API_TOKEN"] = "xxx"
    
    prompt = f"""Extract the job description in a very organised manner. It should have some detail about the company(in few sentences), some detail about the role(in few sentences), responsibilities(all of them as strings only with line breaks), and qualifications (all of them as strings only with line breaks). Additionally. The job description provided is: '{job_description}'. Return the extracted information in a JSON object with the following keys: job_description(it should have keys company_detail,job_summary,responsibilities and qualifications and they should be empty string if no such information is present), contract_type, category, working_type,salary, geo_lat, geo_long and zip_code. Ensure that each key is present in the JSON object even if no information is available. If any detail cannot be fetched from the job description, assign an empty string to the corresponding key. The contract_type should indicate whether the job is permanent or contract. The category should specify the industry of the job (e.g., finance, healthcare). The working_type should denote if the job is remote, onsite, or hybrid.The geo_lat, geo_long and zip_code should be obtained from the {location}. Please provide the output only in the requested JSON format,you do not need to mention that it's json just give the response in json directly. Please ensure the format I can parse into json, ensure you use double quotes and other quotes inside the text be escaped. Please ensure it mandatorily have all the keys company_detail,job_summary,responsibilities and qualifications ,contract_type, category, working_type,salary, geo_lat, geo_long, zip_code. Take care.

Example output:
{{
    "job_description": {{
        "company_detail": "ABC Corp is a leading company in the tech industry, known for its innovative solutions and dynamic work environment.",
        "job_summary": "We are seeking a software engineer to join our team and work on exciting projects.",
        "responsibilities": "Develop software solutions\\nCollaborate with cross-functional teams\\nTroubleshoot and debug applications",
        "qualifications": "Bachelor's degree in Computer Science\\n2+ years of experience in software development\\nProficiency in Python and JavaScript"
    }},
    "contract_type": "Permanent",
    "category": "Technology",
    "working_type": "Remote",
    "salary": "$80,000 - $100,000 per year",
    "geo_lat": "34.0522",
    "geo_long": "-118.2437",
    "zip_code": "90001"
}}
"""
    # async with rate_limiter:
    input_params = {
    "top_p": 0.95,
    "prompt": prompt,
    "temperature": 0,
    "system_prompt": "You are a helpful, respectful and honest assistant and Always respond with a JSON",
    "prompt_template": "system\n\n{system_prompt}user\n\n{prompt}assistant\n\n",
    "presence_penalty": 0,
    "max_tokens": 1200
    }
    try:
        prediction = await replicate.async_run(
            "meta/meta-llama-3-8b-instruct", 
            input=input_params
        )
        # print("prediction",prediction)
        result = ""
        if isinstance(prediction, list):
            result = ''.join(prediction)
        else:
            strData = str(prediction)
            result = ''.join(strData)
        start_index = result.find('{')
        end_index = result.rfind('}') + 1
        if start_index != -1 and end_index != -1:
            json_str = result[start_index:end_index]
            repaired_json_str = repair_json(json_str)
            parsed_json = json.loads(repaired_json_str,strict=False)
            if isinstance(parsed_json, list):
                parsed_json = parsed_json[0]
            data_final = [{
            "uniqueId": data['unique_id'],
            "title": data['position_name'],
            "description": parsed_json.get('job_description', ''),
            "salary": parsed_json['salary'] if data['salary'] == "" else data['salary'],
            "employer": data['company_name'],
            "category": parsed_json.get('category', ''),
            "post_date": data['post_date'],
            "contract_type": parsed_json.get('contract_type', ''),
            "contract_time": data['contract_time'],
            "working_type": parsed_json.get('working_type', ''),
            "location": {
                "location": data['location'],
                "location_raw": "",
                "city": data['city'],
                "state": data['state'],
                "country": data['country'],
                "geo_lat": parsed_json.get("geo_lat", ""),
                "geo_lang": parsed_json.get("geo_long", ""),
                "zip_code": parsed_json.get("zip_code", ""),
            },
            "expirydate": "",
            "jobserviceportals": data['job_service_portals']
            }]
            write_to_file(data_final, 'processed_job_data.json', file_format='json')
            print("written for id",data['unique_id'])
            return data
        else:
            raise ValueError("No JSON object found in the response")
    except Exception as e:
        # logger.error(f"Error extracting job description: {e}")
        print(f"Error extracting job description: {e}")
        print("parsed_json",parsed_json)
        raise e
        return null

def calculate_post_date(post_days):
    if 'day' in post_days:
        days_ago = int(post_days.split()[0])
        return (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    return datetime.now().strftime('%Y-%m-%d')

async def process_job_elements(job_elements,location_data):
    global id_count
    raw_job_data = []
    for listing in job_elements:
        try : 
            position_name = listing.find('h2', class_='KLsYvd').text.strip()
            company_name = listing.find('div', class_='nJlQNd sMzDkb').text.strip()
            location = listing.find('div', class_='tJ9zfc').find_all('div')[1].text.strip()
            if location :
                location_parts = location.split(", ")
            city = location_parts[0] if location_parts else ""
            state = clean_state_name(location_parts[1]) if len(location_parts) > 1 else ""
            country = location_parts[2] if len(location_parts) > 2 else ""
            position_description_span = listing.find('span', class_='HBvzbc')
            inner_span = listing.find('span', class_='WbZuDe')
            # Get text from both spans
            position_description = ""
            if position_description_span:
                position_description += position_description_span.text.strip() + " "
            inner_span_count = 0
            # if inner_span :
            #     position_description += inner_span.text.strip()
            #     print("position_description with inner",position_description)
            employment_span = listing.find('span', {'aria-label': lambda x: x and 'Employment type' in x})
            contract_time = ""
            if employment_span:
                contract_time = employment_span.text.strip()

            post_date_span = listing.find('span', {'aria-label': lambda x: x and 'Posted ' in x})
            post_days = ""
            if post_date_span:
                post_days = post_date_span.text.strip()

            post_date = calculate_post_date(post_days)

            salary_span = listing.find('span', {'aria-label': lambda x: x and 'Salary ' in x})
            salary = ""
            if salary_span:
                salary = salary_span.text.strip()

            job_service_div = listing.find('div', class_='B8oxKe BQC79e xXyUwe')
            job_service_portals = []
            
            if job_service_div:
                anchors = job_service_div.find_all('a')
                for a in anchors:
                    apply_at = a.get('title', '').split(' on ')[-1].strip()
                    url = a['href']
                    job_service_portal = {'applyAt': apply_at, 'url': url}
                    job_service_portals.append(job_service_portal)

            unique_id = f"{id_count}_{position_name[:6]}_{company_name[:6]}_{city}_{state}"
            id_count +=1

            # Extract qualifications, responsibilties and benefits
            qualifications = None
            responsibilties = None
            benefits = None
            qualifications_divs = listing.find_all('div', class_='JxVj3d')
            qualifications_count = 0
            responsibilities_count = 0
            benefits_count = 0

            latitude, longitude = get_latitude_longitude(city, state, location_data)

            

            for div in qualifications_divs:
                header_div = div.find('div', class_='iflMsb')
                if header_div and header_div.text.strip() == 'Qualifications':
                    qualifications_count += 1
                    if qualifications_count == 2:
                        qualifications = div.text.strip()
                        # break
                if header_div and header_div.text.strip() == 'Responsibilities':
                    responsibilities_count += 1
                    if responsibilities_count == 2:
                        responsibilties = div.text.strip()
                        # break
                if header_div and header_div.text.strip() == 'Benefits':
                    benefits_count += 1
                    if benefits_count == 2:
                        benefits = div.text.strip()
                        # break
            # print("qualifications",qualifications)
            # print("responsibilties",responsibilties)
            # print("benefits",benefits)
            data = {
                'unique_id': clean_unique_id(unique_id),
                'title': position_name,
                'employer': company_name,
                'description': {
                    'qualifications': qualifications,
                    'responsibilities': responsibilties,
                    'benefits': benefits,
                    'job_description': position_description
                },
                'location': {
                    "location": clean_location(location),
                    "location_raw": "",
                    "city": city,
                    "state": state,
                    "country": country,
                    "geo_lat": latitude,
                    "geo_lang": longitude,
                },
                'contract_time': contract_time,
                'post_date': post_date,
                "expirydate": "",
                'salary': salary,
                "job_service_portals": job_service_portals
            }
            # print("data",data)
            raw_job_data.append(data)
        except Exception as e:
            print(f"Error processing listing: {e}")
            continue
    return raw_job_data
async def auto_scroll(page,selector):
    try:
        await page.evaluate(f'''
            async function() {{
                const element = document.querySelector('{selector}');
                if (element) {{
                    let totalHeight = 0;
                    const distance = 200;
                    const delay = 500;
                    while (totalHeight < element.scrollHeight) {{
                        element.scrollBy(0, distance);
                        totalHeight += distance;
                        await new Promise(resolve => setTimeout(resolve, delay));
                    }}
                }}
            }}
        ''')
        # logger.info(f"Scrolled element {selector} to the bottom")
        print(f"Scrolled element {selector} to the bottom")
        # logger.info("Extracting Jobs Data after scrolling")
        print("Extracting Jobs Data after scrolling")

        html_content = await page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        job_elements = soup.find_all(class_="pE8vnd avtvi")
        return job_elements
    except Exception as e:
        # logger.error(f"Error scrolling element {selector}: {e}")
        print(f"Error scrolling element {selector}: {e}")

        raise e

async def scrape_page(url, context, location_data):
    # page = await context.new_page()
    page = await context.new_page()
    await context.route('**/*.{png,jpg,jpeg,gif,webp,css,woff,woff2,ttf,svg,eot,ico,mp4,webm,ogg,mp3,wav,pdf,doc,docx,xls,xlsx,ppt,pptx}', lambda route: route.abort())
    try:
        # logger.info(f"Scraping {url}")
        print(f"Scraping {url}")

        await page.goto(url)
        time.sleep(2)
        # await page.screenshot(path='screenshot.png')


        # Check if the element exists and has the text "Sign in"
        sign_in_element = await page.query_selector('//a[contains(@class, "gb_Ea") and contains(@class, "gb_wd") and contains(@class, "gb_nd") and contains(@class, "gb_ne")]')
        if sign_in_element:
            sign_in_text = await sign_in_element.inner_text()
            if sign_in_text.strip() != "Sign in":
                raise Exception(f"Proxy language is not English: Sign-in button text is '{sign_in_text}'")
            
        # Check if the element exists
        element_handle = await page.query_selector('div.zxU94d.gws-plugins-horizon-jobs__tl-lvc')
        if not element_handle:
            raise Exception(f"Element with selector 'div.zxU94d.gws-plugins-horizon-jobs__tl-lvc' not found on {url}")

        # logger.info(f"Element with selector 'div.zxU94d.gws-plugins-horizon-jobs__tl-lvc' found, scrolling down")
        print(f"Element with selector 'div.zxU94d.gws-plugins-horizon-jobs__tl-lvc' found, scrolling down")

        job_elements = await auto_scroll(page, 'div.zxU94d.gws-plugins-horizon-jobs__tl-lvc')
        raw_data = await process_job_elements(job_elements,location_data)
        # print("data",raw_data)
        # write_to_file(data, 'job_data.json')
        # await page.close()
        return raw_data
        
    
        # job_data_list.append(data)
    except Exception as e:
        # logger.error(f"Error scraping {url}: {e}")
        print(f"Error scraping {url}: {e}")

        await page.close()
        raise e
    finally :
        await page.close()

    # await page.close()
async def handle_browser_instance(urls, proxy, location_data):
    chrome_path = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
    async with async_playwright() as p:
        while True:
            try:
                # logger.info("Launching Chrome browser with global proxy")
                print("Launching Chrome browser with global proxy")

                browser = await p.chromium.launch(
                    executable_path=chrome_path,
                    headless=False,
                    proxy={
                        'server': proxy['server'],
                        'username': proxy['username'],
                        'password': proxy['password']
                    },
                    args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-infobars",
                "--window-position=0,0",
                "--ignore-certifcate-errors",
                "--ignore-certifcate-errors-spki-list",
            ],
                )
                # iphone = Playwright.devices["iPhone 6"]
                # context = await browser.new_context(**iphone)
                context = await browser.new_context(bypass_csp=True,
    viewport={'width': 800, 'height': 600})
                main_page = await context.new_page()
                main_page.route(re.compile(r"\.(jpg|png|svg)$"), 
		lambda route: route.abort()) 
                await main_page.goto('about:blank')
                break
            except Exception as e:
                # logger.error(f"Failed to launch Chrome browser with proxy: {e}")
                print(f"Failed to launch Chrome browser with proxy: {e}")

                time.sleep(2)  # Wait before retrying

        for url in urls:
            while True:
                try:
                    raw_data = await scrape_page(url, context, location_data)
                    write_to_file(raw_data, 'raw_job_data.json', file_format='json')
                    print("Saved for",url)
                    # sem = asyncio.Semaphore(RATE_LIMIT)
                    # try : 
                    #     # async with sem:
                    #     #     tasks = []
                    #     #     for data in raw_data:
                    #             # print("Position Description:", data['positionDescription'])
                    #             # print("Location:", data['location'])
                    #             # print("---")  # Separator for readability
                    #         #     task = extract_job_description_with_openai_async(data, sem)
                    #         #     tasks.append(task)
                    #         # processed_data = await asyncio.gather(*tasks)
                    #     # print("processed_data", processed_data)
                    # except Exception as e:
                    #     print(f"Error processing with replicate: {e}")
                    #     continue
                    break  # Exit loop if scraping is successful
                except Exception as e:
                    # logger.error(f"Retry scraping {url} due to error: {e}")
                    print(f"Retry scraping {url} due to error: {e}")

                    await context.close()
                    await browser.close()
                    time.sleep(5)  # Wait before retrying

                    while True:
                        try:
                            # logger.info("Re-launching Chrome browser with new proxy login")
                            print("Re-launching Chrome browser with new proxy login")

                            browser = await p.chromium.launch(
                                executable_path=chrome_path,
                                headless=False,
                                proxy={
                                    'server': proxy['server'],
                                    'username': proxy['username'],
                                    'password': proxy['password']
                                }
                            )
                            context = await browser.new_context(bypass_csp=True,
    viewport={'width': 800, 'height': 600})
                            main_page = await context.new_page()
                            await main_page.goto('about:blank')
                            break
                        except Exception as e:
                            # logger.error(f"Failed to re-launch Chrome browser with proxy: {e}")
                            print(f"Failed to re-launch Chrome browser with proxy: {e}")

                            time.sleep(5)  

        await context.close()
        await browser.close()

async def scrape_and_save_raw_data():
    # with open('data.json', 'r') as f:
    #     cities_json = json.load(f)
    location_data = []
    with open('output.json', 'r') as f:
        location_data = json.load(f)
    urls = [
        f"https://www.google.com/search?q=jobs+in+{city_data['name']}+{city_data['state_name']}&ibp=htl;jobs&sa=X"
        for city_data in location_data
        if city_data.get('name') and city_data.get('state_name')
    ]
    # print(urls,"urls")

    proxy = {
        'server': 'proxy.apify.com:8000',  # Replace with your proxy server
        # 'server': f"http://groups-RESIDENTIAL:{'apify_proxy_dsGYtfqZ67wRGZRK6IYVzqbAgTbLMz1laqBi'}@proxy.apify.com:8000",
        'username': 'groups-RESIDENTIAL',
        'password': 'apify_proxy_dsGYtfqZ67wRGZRK6IYVzqbAgTbLMz1laqBi'
    }
    # Split URLs into chunks of 10
    chunks = [urls[i:i + 20] for i in range(0, len(urls), 20)]
    
    # Limit to 10 concurrent browser instances
    semaphore = asyncio.Semaphore(2)

    job_data_list = []
    async def semaphore_wrapper(chunk):
        async with semaphore:
            await handle_browser_instance(chunk, proxy, location_data)

    tasks = [semaphore_wrapper(chunk) for chunk in chunks]

    # Start monitoring CPU and memory usage
    # async def monitor_resources():
    #     while True:
    #         cpu_usage = psutil.cpu_percent(interval=1)
    #         memory_usage = psutil.virtual_memory().percent
    #         logger.info(f"Current CPU Usage: {cpu_usage}%, Memory Usage: {memory_usage}%")
    #         await asyncio.sleep(1)  # Adjust the frequency as needed

    # Run the monitoring in the background
    # monitor_task = asyncio.create_task(monitor_resources())
    await asyncio.gather(*tasks)
    # monitor_task.cancel()
async def main():
    # Initialize batch number
    batch_number = 1

    while True:
        # Create a filename based on the current batch number
        filename = "batch_record.txt"

        # Open the file in append mode and write the batch information
        with open(filename, 'w') as file:
            file.write(f"batch{batch_number}\n")

        # Call the original function
        await scrape_and_save_raw_data()

        # Increment the batch number for the next run
        batch_number += 1

        # Sleep for one week
        await asyncio.sleep(7 * 24 * 60 * 60)
if __name__ == '__main__':
    asyncio.run(main())

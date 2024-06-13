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

# Configure logging
# logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
MAX_REQUESTS_PER_MINUTE = 600
RATE_LIMIT = int(MAX_REQUESTS_PER_MINUTE / 60)

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
                return {
                    "job_description": {"company_detail": "", "job_summary": "", "responsibilities": "", "qualifications": ""},
                    "contract_type": "",
                    "category": "",
                    "working_type": "",
                    "salary": "",
                    "geo_lat": "",
                    "geo_long": "",
                    "zip_code": ""
                }

    
async def extract_job_description_with_openai(data):
    job_description = data['position_description']
    location = data['location']
    os.environ["REPLICATE_API_TOKEN"] = "r8_Dnn5HXRf8ffsWwNcIa9mU96Nz5r3WMh2x7VmO"
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
        try:
            prediction = await replicate.async_run(
                "meta/meta-llama-3-8b-instruct", 
                input=input_params
            )
        except Exception as e:
            print(f"An error occurred: {e}")
        # print("prediction",prediction)
        result = ''.join(prediction)
        start_index = result.find('{')
        end_index = result.rfind('}') + 1
        
        if start_index != -1 and end_index != -1:
            json_str = result[start_index:end_index]
            repaired_json_str = repair_json(json_str)
            parsed_json = json.loads(repaired_json_str,strict=False)
            data = [{
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
            write_to_file(data, 'processed_job_data.json', file_format='json')


            return parsed_json
        else:
            raise ValueError("No JSON object found in the response")
    except Exception as e:
        # logger.error(f"Error extracting job description: {e}")
        print(f"Error extracting job description: {e}")
        raise e

def calculate_post_date(post_days):
    if 'day' in post_days:
        days_ago = int(post_days.split()[0])
        return (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    return datetime.now().strftime('%Y-%m-%d')

async def process_job_elements(job_elements):
    raw_job_data = []
    for listing in job_elements:
        position_name = listing.find('h2', class_='KLsYvd').text.strip()
        company_name = listing.find('div', class_='nJlQNd sMzDkb').text.strip()
        location = listing.find('div', class_='tJ9zfc').find_all('div')[1].text.strip()
        location_parts = location.split(", ")
        city = location_parts[0] if len(location_parts) > 0 else ""
        state = location_parts[1] if len(location_parts) > 1 else ""
        country = location_parts[2] if len(location_parts) > 2 else ""
        position_description_span = listing.find('span', class_='HBvzbc')
        inner_span = listing.find('span', class_='WbZuDe')
        # Get text from both spans
        position_description = ""
        if position_description_span:
            position_description += position_description_span.text.strip() + " "
        if inner_span:
            position_description += inner_span.text.strip()
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
        anchors = job_service_div.find_all('a')
        for a in anchors:
            apply_at = a.get('title', '').split(' on ')[-1].strip()
            url = a['href']
            job_service_portal = {'applyAt': apply_at, 'url': url}
            job_service_portals.append(job_service_portal)

        unique_id = '_'.join(word[:3].upper() + word[-3:].upper() for word in [position_name, company_name,city,state])

        data = {
            'unique_id': unique_id,
            'position_name': position_name,
            'company_name': company_name,
            'location': location,
            'city': city,
            'state': state,
            'country': country,
            'position_description': position_description,
            'contract_time': contract_time,
            'post_date': post_date,
            'salary': salary,
            "job_service_portals": job_service_portals
        }
        raw_job_data.append(data)
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

async def scrape_page(url, context, job_data_list):
    page = await context.new_page()
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
        raw_data = await process_job_elements(job_elements)
        # print("data",raw_data)
        # write_to_file(data, 'job_data.json')
        return raw_data
        
    
        # job_data_list.append(data)
    except Exception as e:
        # logger.error(f"Error scraping {url}: {e}")
        print(f"Error scraping {url}: {e}")

        await page.close()
        raise e

    # await page.close()
async def handle_browser_instance(urls, proxy, job_data_list):
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
                context = await browser.new_context()

                break
            except Exception as e:
                # logger.error(f"Failed to launch Chrome browser with proxy: {e}")
                print(f"Failed to launch Chrome browser with proxy: {e}")

                time.sleep(5)  # Wait before retrying

        for url in urls:
            while True:
                try:
                    raw_data = await scrape_page(url, context, job_data_list)
                    write_to_file(raw_data, 'raw_job_data.json', file_format='json')
                    sem = asyncio.Semaphore(RATE_LIMIT)
                    try : 
                        async with sem:
                            print("sem",sem)
                            tasks = []
                            for data in raw_data:
                                # print("Position Description:", data['positionDescription'])
                                print("Location:", data['location'])
                                print("---")  # Separator for readability
                                task = extract_job_description_with_openai_async(data, sem)
                                tasks.append(task)
                            processed_data = await asyncio.gather(*tasks)
                        print("here2")
                        print("processed_data", processed_data)
                    except Exception as e:
                        print(f"Error processing with replicate: {e}")
                        continue
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
                            context = await browser.new_context()
                            break
                        except Exception as e:
                            # logger.error(f"Failed to re-launch Chrome browser with proxy: {e}")
                            print(f"Failed to re-launch Chrome browser with proxy: {e}")

                            time.sleep(5)  

        await context.close()
        await browser.close()

async def scrape_and_save_raw_data():
    with open('data.json', 'r') as f:
        cities_json = json.load(f)
    urls = [
        f"https://www.google.com/search?q=jobs+in+{city_data['name']}+{city_data['state_name']}&ibp=htl;jobs&sa=X"
        for city_data in cities_json
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
    chunks = [urls[i:i + 10] for i in range(0, len(urls), 10)]
    
    # Limit to 10 concurrent browser instances
    semaphore = asyncio.Semaphore(2)

    job_data_list = []
    async def semaphore_wrapper(chunk):
        async with semaphore:
            await handle_browser_instance(chunk, proxy, job_data_list)

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
    await scrape_and_save_raw_data()
if __name__ == '__main__':
    asyncio.run(main())

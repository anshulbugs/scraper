import asyncio
import psutil
# from playwright.sync_api import Playwright
from playwright.async_api import async_playwright, Playwright
import logging
import time
from datetime import datetime, timedelta
from bs4 import BeautifulSoup
import json
from dicttoxml import dicttoxml
import replicate
import os
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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
        logger.error(f"Error writing to file {filename}: {e}")


async def extract_job_description_with_openai_async(job_description, location, semaphore, max_retries=3, retry_delay=2):
    async with semaphore:
        return await retry_async(
            extract_job_description_with_openai, 
            job_description, location, max_retries=max_retries, retry_delay=retry_delay
        )

async def retry_async(func, *args, max_retries=3, retry_delay=2):
    attempt = 0
    while attempt < max_retries:
        try:
            return await func(*args)
        except Exception as e:
            logger.error(f"Error on attempt {attempt + 1}: {e}")
            attempt += 1
            if attempt < max_retries:
                await asyncio.sleep(retry_delay * (2 ** attempt))
            else:
                logger.error("Max retries reached. Returning fallback response.")
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

    
async def extract_job_description_with_openai(job_description,location):
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


    

    input_params = {
    "top_p": 0.95,
    "prompt": prompt,
    "temperature": 0,
    "system_prompt": "You are a helpful, respectful and honest assistant and Always respond with a JSON",
    "prompt_template": "system\n\n{system_prompt}user\n\n{prompt}assistant\n\n",
    "presence_penalty": 0,
    "max_tokens": 1500
    }
    try:
        prediction = await replicate.async_run(
        "meta/meta-llama-3-70b-instruct",
        input=input_params
        )
        result = ''.join(prediction)
        parsed_json = json.loads(result)
        return parsed_json
    except Exception as e:
        print(f"Error extracting job description: {e}")

        logger.error(f"Error extracting job description: {e}")
        raise e

def calculate_post_date(post_days):
    if 'day' in post_days:
        days_ago = int(post_days.split()[0])
        return (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%d')
    return datetime.now().strftime('%Y-%m-%d')

async def process_job_elements(job_elements):
    job_data = []
    # for listing in job_elements:
    #     position_name = listing.find('h2', class_='KLsYvd').text.strip()
    #     print("position_name",position_name)
    #     company_name = listing.find('div', class_='nJlQNd sMzDkb').text.strip()
    #     print("company_name",company_name)
    #     location = listing.find('div', class_='tJ9zfc').find_all('div')[1].text.strip()
    #     print("location",location)
    #     # Split the location string by comma
    #     location_parts = location.split(", ")

    #     # Extract city, state, and country
    #     city = location_parts[0]
    #     state = location_parts[1]
    #     country = location_parts[2]

    #     # unique_id = '_'.join(word[:3].upper() for word in [position_name, company_name, ''.join(location.split())])
    #     # Find position description spans
    #     unique_id = '_'.join(word[:3].upper() + word[-3:].upper() for word in [position_name, company_name] + location_parts)

    #     position_description_span = listing.find('span', class_='HBvzbc')
    #     inner_span = listing.find('span', class_='WbZuDe')
        
    #     # Get text from both spans
    #     position_description = ""
    #     if position_description_span:
    #         position_description += position_description_span.text.strip() + " "
    #     if inner_span:
    #         position_description += inner_span.text.strip()
    #     # print("position_description",position_description)
        
    #     employment_span = listing.find('span', {'aria-label': lambda x: x and 'Employment type' in x})
    #     contract_time = ""
    #     # Extract the contract_time if the span exists
    #     if employment_span:
    #         # Extract the text content from the span
    #         contract_time = employment_span.text.strip()
    #         print("Contract Time:", contract_time)
    #     else:
    #         print("Contract Time:", contract_time)
    #         print("Employment type information not found.")


    #     post_date_span = listing.find('span', {'aria-label': lambda x: x and 'Posted ' in x})
    #     post_days = ""


    #     # Extract the post_date  if the span exists
    #     if post_date_span:
    #         # Extract the text content from the span
    #         post_days = post_date_span.text.strip()
    #         print("post_days:", post_days)
    #     else:
    #         print("Post days:", post_days)
    #         print("Post date information not found.")                
    #     post_date = calculate_post_date(post_days)
    #     print("Post date:", post_date)
    #     salary_span = listing.find('span', {'aria-label': lambda x: x and 'Salary ' in x})
    #     salary = ""


    #     # Extract the post_date  if the span exists
    #     if salary_span:
    #         # Extract the text content from the span
    #         salary = salary_span.text.strip()
    #         print("salary:", salary)
    #     else:
    #         print("salary:", salary)
    #         print("salary information not found.")                 

    #     # Find the div with class "B8oxKe BQC79e xXyUwe"
    #     job_service_div = listing.find('div', class_='B8oxKe BQC79e xXyUwe')
    #     # Initialize an empty list to store job service portals
    #     job_service_portals = []
    #     # print("job_service_div",job_service_div)
        
    #     anchors = job_service_div.find_all('a')
    #     # print("anchors",anchors)
    #     # Iterate over found spans
    #     for a in anchors:
    #         # Extract the 'applyAt' value from the span text
    #         apply_at = a.get('title', '').split(' on ')[-1].strip()

    #         # Extract the URL from the href attribute of the span
    #         url = a['href']

    #         # Create a JSON object for the current job service portal
    #         job_service_portal = {'applyAt': apply_at, 'url': url}

    #         # Append the JSON object to the list
    #         job_service_portals.append(job_service_portal)
    #     data_from_openAI = await extract_job_description_with_openai(position_description,location)
    #     job_data.append({
    #             'unique_id': unique_id,
    #             'title': position_name,
    #             "description": data_from_openAI.get('job_description', ''),
    #             "salary": data_from_openAI['salary'] if salary == "" else salary,
    #             'employer': company_name,
    #             "category": data_from_openAI.get('category', ''),
    #             'post_date': post_date,
    #             "contract_type": data_from_openAI.get('contract_type', ''),
    #             "contract_time": contract_time,
    #             "working_type": data_from_openAI.get('working_type', ''),
    #             "location": {
    #                             "location": location,
    #                             "location_raw": "",
    #                             "city": city,
    #                             "state": state,
    #                             "country": country,
    #                             "geo_lat": data_from_openAI.get("geo_lat", ""),
    #                             "geo_lang": data_from_openAI.get("geo_long", ""),
    #                             "zip_code": data_from_openAI.get("zip_code", ""),
    #                         },
                
    #             'position_description': position_description,
    #             "expirydate": "",
    #             'job_service_portals': job_service_portals
    #         })
    tasks = []
    semaphore = asyncio.Semaphore(len(job_elements))
    for listing in job_elements:
        position_name = listing.find('h2', class_='KLsYvd').text.strip()
        print("Found ", position_name)
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
        # Call the asynchronous OpenAI API function
        task = asyncio.create_task(extract_job_description_with_openai_async(position_description, location,semaphore))
        tasks.append(task)
    # data_from_openAI_list = await asyncio.gather(*tasks)

        # Process the data from OpenAI
    for i, (listing,task) in enumerate(zip(job_elements, tasks)):
        position_name = listing.find('h2', class_='KLsYvd').text.strip()
        company_name = listing.find('div', class_='nJlQNd sMzDkb').text.strip()
        location = listing.find('div', class_='tJ9zfc').find_all('div')[1].text.strip()
        location_parts = location.split(", ")
        city = location_parts[0] if len(location_parts) > 0 else ""
        state = location_parts[1] if len(location_parts) > 1 else ""
        country = location_parts[2] if len(location_parts) > 2 else ""

        # unique_id = '_'.join(word[:3].upper() + word[-3:].upper() for word in [position_name, company_name])
        unique_id = '_'.join(word[:3].upper() + word[-3:].upper() for word in [position_name, company_name,city,state])
        print("unique_id",unique_id)
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
        
        data_from_openAI = await task
            # print("task",task)

        data = {
            "uniqueId": unique_id,
            "title": position_name,
            "company": company_name,
            "description": data_from_openAI.get('job_description', ''),
            "salary": data_from_openAI['salary'] if salary == "" else salary,
            "employer": company_name,
            "category": data_from_openAI.get('category', ''),
            "post_date": post_date,
            "contract_type": data_from_openAI.get('contract_type', ''),
            "contract_time": contract_time,
            "working_type": data_from_openAI.get('working_type', ''),
            "location": {
                "location": location,
                "location_raw": "",
                "city": city,
                "state": state,
                "country": country,
                "geo_lat": data_from_openAI.get("geo_lat", ""),
                "geo_lang": data_from_openAI.get("geo_long", ""),
                "zip_code": data_from_openAI.get("zip_code", ""),
            },
            "expirydate": "",
            "jobserviceportals": job_service_portals
        }
        job_data.append(data)

    return job_data
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
        logger.info(f"Scrolled element {selector} to the bottom")
        logger.info("Extracting Jobs Data after scrolling")
        html_content = await page.content()
        soup = BeautifulSoup(html_content, 'html.parser')
        job_elements = soup.find_all(class_="pE8vnd avtvi")
        return job_elements
    except Exception as e:
        logger.error(f"Error scrolling element {selector}: {e}")
        raise e

async def scrape_page(url, context, job_data_list):
    page = await context.new_page()
    try:
        logger.info(f"Scraping {url}")
        await page.goto(url)
        time.sleep(2)
        await page.screenshot(path='screenshot.png')

        # Check if the element exists
        element_handle = await page.query_selector('div.zxU94d.gws-plugins-horizon-jobs__tl-lvc')
        if not element_handle:
            raise Exception(f"Element with selector 'div.zxU94d.gws-plugins-horizon-jobs__tl-lvc' not found on {url}")

        logger.info(f"Element with selector 'div.zxU94d.gws-plugins-horizon-jobs__tl-lvc' found, scrolling down")
        job_elements = await auto_scroll(page, 'div.zxU94d.gws-plugins-horizon-jobs__tl-lvc')
        data = await process_job_elements(job_elements)
        # write_to_file(data, 'job_data.json')
        write_to_file(data, 'job_data.xml', file_format='xml')
        data.clear()
        job_data_list.append(data)
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        await page.close()
        raise e

    # await page.close()
async def handle_browser_instance(urls, proxy, job_data_list):
    chrome_path = "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe"
    async with async_playwright() as p:
        while True:
            try:
                logger.info("Launching Chrome browser with global proxy")
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
                logger.error(f"Failed to launch Chrome browser with proxy: {e}")
                time.sleep(5)  # Wait before retrying

        for url in urls:
            while True:
                try:
                    await scrape_page(url, context, job_data_list)
                    break  # Exit loop if scraping is successful
                except Exception as e:
                    logger.error(f"Retry scraping {url} due to error: {e}")
                    await context.close()
                    await browser.close()
                    time.sleep(5)  # Wait before retrying

                    while True:
                        try:
                            logger.info("Re-launching Chrome browser with new proxy login")
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
                            logger.error(f"Failed to re-launch Chrome browser with proxy: {e}")
                            time.sleep(5)  

        await context.close()
        await browser.close()
async def main():
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
        'username': 'groups-RESIDENTIAL',          # Replace with your proxy username
        'password': 'apify_proxy_dsGYtfqZ67wRGZRK6IYVzqbAgTbLMz1laqBi'          # Replace with your proxy password
    }
    # Split URLs into chunks of 10
    chunks = [urls[i:i + 10] for i in range(0, len(urls), 10)]
    
    # Limit to 10 concurrent browser instances
    semaphore = asyncio.Semaphore(5)

    job_data_list = []
    async def semaphore_wrapper(chunk):
        async with semaphore:
            await handle_browser_instance(chunk, proxy, job_data_list)

    tasks = [semaphore_wrapper(chunk) for chunk in chunks]

    # Start monitoring CPU and memory usage
    async def monitor_resources():
        while True:
            cpu_usage = psutil.cpu_percent(interval=1)
            memory_usage = psutil.virtual_memory().percent
            logger.info(f"Current CPU Usage: {cpu_usage}%, Memory Usage: {memory_usage}%")
            await asyncio.sleep(1)  # Adjust the frequency as needed

    # Run the monitoring in the background
    monitor_task = asyncio.create_task(monitor_resources())
    await asyncio.gather(*tasks)
    monitor_task.cancel()

if __name__ == '__main__':
    asyncio.run(main())

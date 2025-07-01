import asyncio
import json
import time
import random
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from openai import OpenAI
import re
from kafka import KafkaProducer

AUTH = '<UR DATABRIGHT KEY>'
SBR_WS_CDP = f'<from site>'

# client = OpenAI(
#     api_key="<AI-API>",
#     base_url="https://api.deepseek.com/v1"
# )

BASE_URL = "https://zoopla.co.uk"
LOCATION= "London"


def extract_picture(li_elements):
    image_urls = []

    for li in li_elements:
        picture = li.find('picture')
        if picture:
            sources = picture.find_all('source')
            for source in sources:
                if 'srcset' in source.attrs:
                    url = source['srcset'].split()[0]
                    if '1024' in url and url not in image_urls:
                        image_urls.append(url)

    return image_urls


def prepare_property_input(soup_element):
    """Extract clean text from BeautifulSoup element for AI processing"""
    if not soup_element:
        return ""
    for unwanted in soup_element.find_all(['button', 'svg', 'a', 'dialog']):
        unwanted.decompose()
    text = soup_element.get_text(' ', strip=True)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()



def extract_property_details(input_text):
    try:
        response = client.chat.completions.create(
            model="deepseek-chat",  # Their flagship model
            messages=[{
                "role": "user",
                "content": f"""
                    Extract UK property details as JSON from this text.
                    Return ONLY valid JSON with these keys:
                    - price, address, bedrooms, bathrooms, tenure, EPC Rating
                    Text: {input_text}
                    """
            }],
            response_format={"type": "json_object"},
            temperature=0.3  # Less creative → better extraction
        )
        return json.loads(response.choices[0].message.content)
    except Exception as e:
        print(f"DeepSeek Error: {e}")
        return None

async def extract_floor_plan(page):
    await page.click('button._194zg6t9._13uwpzl3')
    # Wait for floor plan to load
    await page.wait_for_selector('div[aria-label="Floor plan images"]', timeout=60000)

    # Get the floor plan container
    floor_plan_container = await page.query_selector('div[aria-label="Floor plan images"]')
    floor_plan_html = await floor_plan_container.inner_html()
    soup = BeautifulSoup(floor_plan_html, 'html.parser')
    # Extract all 1200w images
    floor_plans = []
    for picture in soup.find_all('picture', class_='_15j4h5e4'):
        for source in picture.find_all('source'):
            srcset = source.get('srcset', '')
            if '1200w' in srcset:
                url = srcset.split()[0].replace(':p', '')
                if url not in floor_plans:
                    floor_plans.append(url)

    return floor_plans


def extract_tenure_info(ground_rent):
    """
    Extracts all tenure-related information from the text
    including the two missing fields (service_charge and Council tax band)
    """
    data = {
        "tenure": "",
        "time_remaining_on_lease": "",
        "service_charge": "",
        "Council tax band": "",
        "Ground rent": "",
        "Ground rent review": ""
    }

    # More robust regex patterns that handle your specific format
    patterns = {
        "tenure": r"tenure[:]?\s*(\w+)",
        "time_remaining_on_lease": r"\((\d+)\s*years\)",
        "service_charge": r"service charge[:]?\s*(£[\d,]+(?:\sper\syear)?|\w+\s\w+)",
        "Council tax band": r"council tax band[:]?\s*([A-Z])",
        "Ground rent": r"ground rent[:]?\s*(£[\d,]+)",
        "Ground rent review": r"ground rent date of next review[:]?\s*(.+)"
    }

    for field, pattern in patterns.items():
        match = re.search(pattern, ground_rent, re.IGNORECASE)
        if match:
            data[field] = match.group(1).strip()

    return data

async def run(pw, producer):
    print('Connecting to Scraping Browser...')
    browser = await pw.chromium.connect_over_cdp(SBR_WS_CDP)
    try:
        page = await browser.new_page()
        print(f'Connected! Navigating to {BASE_URL}')
        await page.goto(BASE_URL)

        # CAPTCHA handling
        client = await page.context.new_cdp_session(page)
        print('Waiting for CAPTCHA to solve...')
        solve_res = await client.send('Captcha.waitForSolve', {
            'detectTimeout': 10000,
        })
        print('CAPTCHA solve status:', solve_res['status'])

        print('Navigated! Scraping page content...')

        #enter london in the search bar ans press enter to search
        await page.fill('input[name="autosuggest-input"]', LOCATION)
        await page.keyboard.press("Enter")
        print("Waiting for search results...")
        await page.wait_for_load_state("load")

        content = await page.inner_html('div[data-testid="regular-listings"]')
        #access all the items of realstates
        soup = BeautifulSoup(content, "html.parser")

        for idx, div in enumerate(soup.find_all("div", class_="dkr2t86")):
            data = {}

            p_tag = div.find("p", class_=lambda x: x and "m6hnz63" in x).text
            address=div.find('address').text
            link= div.find('a')['href'] #to look in details

            data.update({
                "address":address,
                "description":p_tag,
                "link":BASE_URL+link
            })

            #go to listing page
            await page.goto(data['link'])
            await page.wait_for_timeout(60000)
            await page.wait_for_load_state("load")

            content = await page.inner_html('div[class="_1olqsf91"]')
            soup = BeautifulSoup(content, "html.parser")
            all_pictures=soup.find("ol", {"aria-label":"Gallery images"})
            pictures = extract_picture(all_pictures)
            data['pictures'] = pictures
            #print(soup)
            price = soup.find("p", {"class": "_194zg6t3 r4q9to1"}).text
            # print(price)
            data["price"] = price

            details_list = soup.find('ul', class_='_1wmbmfq1')
            if details_list:
                for item in details_list.find_all('li'):
                    text = item.get_text(strip=True).lower()
                    if 'bed' in text:
                        data['bedrooms'] = text.split()[0]
                    elif 'bath' in text:
                        data['bathrooms'] = text.split()[0]
                    elif 'reception' in text:
                        data['receptions'] = text.split()[0]
            # Get all text content for regex searching
            print("getting all text")

            ground_rent = soup.find("ul", {"class":"_1khto1l1"}).text.strip(" ")
            infos = extract_tenure_info(ground_rent)
            # print(infos)
            data.update(infos)
            # print(data)
            #tenure - > p class class="_194zg6t8 _1khto1l6"
            #council -> p class _194zg6t8 _1khto1l6
            #ground rent -> _194zg6t8 _1khto1l3

            # floor_plan = extract_floor_plan(page)
            # print(floor_plan)
            # data.update(floor_plan)
            # data.update(property_details)
            print(data)
            print("-----------Sending data to kafka------------")
            producer.send("properties", value=json.dumps(data).encode('utf-8'))
            print("data sent to kafka")
            break

        # html = await page.content()
        # print(html)
    finally:
        await browser.close()

async def main():
    producer = KafkaProducer(bootstrap_servers=["localhost:9092"], max_block_ms=6000)
    async with async_playwright() as playwright:
        await run(playwright, producer)

if __name__ == '__main__':
    asyncio.run(main())

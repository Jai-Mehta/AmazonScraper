import csv
import json
from time import time, sleep
from datetime import datetime
from random import randint, choice
from requests import get
from warnings import warn


def get_all_jobs(pages, existing_jobs):
    requests = 0
    start_time = time()
    total_runtime = datetime.now()
    user_agent_list = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:50.0) Gecko/20100101 Firefox/50.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    ]

    for page in pages:
        try:
            user_agent = choice(user_agent_list)
            headers = {'User-Agent': user_agent}

            response = get('https://www.amazon.jobs/en/search.json?base_query=&city=&country=USA&county=&'
                           'facets%5B%5D=location&facets%5B%5D=business_category&facets%5B%5D=category&'
                           'facets%5B%5D=schedule_type_id&facets%5B%5D=employee_class&facets%5B%5D=normalized_location'
                           '&facets%5B%5D=job_function_id&job_function_id%5B%5D=job_function_corporate_80rdb4&'
                           'latitude=&loc_group_id=&loc_query=USA&longitude=&'
                           'normalized_location%5B%5D=Seattle%2C+Washington%2C+USA&'
                           'normalized_location%5B%5D=San+Francisco'
                           '%2C+California%2C+USA&normalized_location%5B%5D=Sunnyvale%2C+California%2C+USA&'
                           'normalized_location%5B%5D=Bellevue%2C+Washington%2C+USA&'
                           'normalized_location%5B%5D=East+Palo+Alto%2C+California%2C+USA&'
                           'normalized_location%5B%5D=Santa+Monica%2C+California%2C+USA&offset={}&query_options=&'
                           'radius=24km&region=&result_limit=10&schedule_type_id%5B%5D=Full-Time&'
                           'sort=recent'.format(page),
                           headers=headers)

            requests += 1
            sleep(randint(8, 15))  # Pause to avoid rate-limiting
            current_time = time()
            elapsed_time = current_time - start_time
            print(f"Amazon Request: {requests}; Frequency: {requests / elapsed_time:.2f} req/s; Total Run Time: {datetime.now() - total_runtime}")

            if response.status_code != 200:
                warn(f"Request: {requests}; Status code: {response.status_code}")
                continue

            yield from get_job_infos(response, existing_jobs)

            if requests > 5:
                warn("Request limit exceeded.")
                break

        except Exception as e:
            print(f"Error occurred: {e}")
            continue


def get_job_infos(response, existing_jobs):
    amazon_jobs = json.loads(response.text)
    today = datetime.now().strftime("%B %d, %Y")  # Get today's date

    for website in amazon_jobs['jobs']:
        site = website['company_name']
        title = website['title']
        location = website['normalized_location']
        job_link = 'https://www.amazon.jobs' + website['job_path']
        posted_date = website.get('posted_date', '')

        # Normalize and parse dates for comparison
        try:
            parsed_posted_date = datetime.strptime(posted_date, "%B %d, %Y")
            parsed_today_date = datetime.strptime(today, "%B %d, %Y")
        except ValueError:
            continue  # Skip if the date format is invalid

        # Skip jobs not posted today
        if parsed_posted_date.date() != parsed_today_date.date():
            continue

        # Skip jobs already in the existing dataset
        if job_link not in existing_jobs:
            yield site, title, location, job_link, posted_date, "No"  # Default Notify value: "No"


def load_existing_jobs(csv_file):
    """Load existing job links from the CSV file."""
    existing_jobs = set()
    try:
        with open(csv_file, 'r', encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                existing_jobs.add(row['Job URL'])  # Use Job URL as a unique identifier
    except FileNotFoundError:
        print(f"{csv_file} not found. Starting fresh.")
    return existing_jobs


def save_jobs_to_csv(csv_file, jobs):
    """Append new jobs to the CSV file."""
    with open(csv_file, 'a', newline='', encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerows(jobs)


def main():
    csv_file = 'amazon_jobs.csv'
    pages = [str(i) for i in range(0, 9990, 10)]

    # Load existing jobs
    existing_jobs = load_existing_jobs(csv_file)

    # Get new jobs and avoid duplicates
    new_jobs = list(get_all_jobs(pages, existing_jobs))

    if new_jobs:
        # Write headers if the file is new
        try:
            with open(csv_file, 'r', encoding="utf-8") as file:
                pass
        except FileNotFoundError:
            with open(csv_file, 'w', newline='', encoding="utf-8") as file:
                writer = csv.writer(file)
                writer.writerow(["Website", "Title", "Location", "Job URL", "Posted Date", "Notify"])  # Header

        # Save new jobs
        save_jobs_to_csv(csv_file, new_jobs)
        print(f"Added {len(new_jobs)} new jobs to {csv_file}.")
    else:
        print("No new jobs found.")


if __name__ == "__main__":
    main()

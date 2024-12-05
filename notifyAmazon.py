import csv
import asyncio
import os
from telegram import Bot
import nest_asyncio

# Apply nest_asyncio to allow nested event loops in environments like Jupyter
nest_asyncio.apply()

# Use environment variables for Telegram credentials
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID')

async def send_telegram_message(bot, message):
    """Send a message via Telegram asynchronously."""
    await bot.send_message(chat_id=TELEGRAM_CHAT_ID, text=message)


async def process_jobs(csv_file):
    """Process jobs from the CSV file and send Telegram messages for new jobs."""
    bot = Bot(token=TELEGRAM_BOT_TOKEN)
    all_jobs = []
    jobs_to_notify = []

    # Read all jobs and identify jobs to notify
    with open(csv_file, 'r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            if row['Notify'].strip() == "No":  # Only process jobs not yet notified
                jobs_to_notify.append(row)
                row['Notify'] = "Yes"  # Mark job as notified
            all_jobs.append(row)  # Preserve all jobs

    # Notify via Telegram
    for job in jobs_to_notify:
        message = (
            f"Company: {job['Website']}\n"
            f"Job Title: {job['Title']}\n"
            f"Location: {job['Location']}\n"
            f"Job Link: {job['Job URL']}\n"
            f"Posted Date: {job['Posted Date']}"
        )
        await send_telegram_message(bot, message)

    # Write all jobs back to the CSV file with updated Notify values
    with open(csv_file, 'w', newline='', encoding='utf-8') as file:
        fieldnames = ['Website', 'Title', 'Location', 'Job URL', 'Posted Date', 'Notify']
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(all_jobs)


def main():
    csv_file = 'amazon_jobs.csv'
    asyncio.run(process_jobs(csv_file))


if __name__ == "__main__":
    main()

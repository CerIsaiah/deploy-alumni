from handshake_helper import construct_url, search_handshake, get_descriptions
import logging
import pandas as pd
from jobspy import scrape_jobs
import asyncio
import openai_async
import os
from dotenv import load_dotenv

load_dotenv()  
_OPEN_AI_API_KEY = os.getenv('OPENAI_API_KEY')

async def get_openai_response(job_description):
    system_prompt = """ANSWER UNDER 100 WORDS!!! ANSWER UNDER 70 WORDS!!! MOST IMPORTANT DETAILS OF JOB DESCRIPTION. BULLET POINT FORMAT
    BULLET POINT FORMAT IS
    \ (Name of Company)
    \ (Name of position)
    \ (Things they are looking for)
    \ (Skills required)
    \ (Pay rate)
    \ (Any special requriements to apply)

    MAKE SURE YOU PUT THE \ IN FRONT OF EACH FORMAT POINT. DONT INCLUDE THE TITLE OF THE THING IN THE () JUST THE CONTENT REQUIRED.
    DONT INCLUDE THE TITLE OF THE THING IN THE () JUST THE CONTENT REQUIRED.
    FOLLOW THIS FORMAT TO ANSWER THE QUESTION!!
    """
    response = await openai_async.chat_complete(
        _OPEN_AI_API_KEY,
        timeout=20,
        payload={
            "model": "gpt-3.5-turbo",
            "messages": [
                {"role": "system", "content": f"{system_prompt}"},
                {"role": "user", "content": "ANSWER UNDER 100 WORDS!!! Here is the job description to summarize in bullet: " + job_description},
            ],
            "temperature": 0.0,
        },
    )
    job_description = response.json()["choices"][0]["message"]['content']
    return job_description

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def handshake_job_search(search_term, job_type):
    try:
        job_types_mapping = {
            'internship': '3',
            'fulltime': None,  # Assuming no job type for full-time in handshake
            'parttime': None  # Assuming no job type for part-time in handshake
        }
        employment_type_mapping = {
            'fulltime': 'Full-Time',
            'parttime': 'Part-Time'
        }

        job_types = job_types_mapping.get(job_type)
        employment_type = employment_type_mapping.get(job_type)

        handshake_url = construct_url(query=search_term, job_type=job_types, employment_type=employment_type, per_page=35)
        save_ids = search_handshake(handshake_url)  # Assume synchronous version
        handshake_jobs = get_descriptions(save_ids)  # Assume synchronous version
        return handshake_jobs
    except Exception as e:
        logger.error(f"Error during Handshake job search: {e}", exc_info=True)
        return pd.DataFrame()

def indeed_job_search(search_term, job_type):
    try:
        return scrape_jobs(
            site_name=["indeed"],
            search_term=search_term,
            results_wanted=50,
            country_indeed="USA",
            job_type=job_type,
            linkedin_fetch_description=False,
        )
    except Exception as e:
        logger.error(f"Error fetching Indeed jobs: {e}")
        return pd.DataFrame()

async def get_openai_response_with_delay(description, delay):
    logging.info(f"Getting OpenAI response for description")
    await asyncio.sleep(delay)
    return await get_openai_response(description)

async def add_description(jobs_df, delay=0.5):
    logging.info("Adding OpenAI descriptions to jobs")
    tasks = [get_openai_response_with_delay(desc, delay) for desc in jobs_df['description']]
    
    openai_descriptions = await asyncio.gather(*tasks)

    # Ensure None or invalid responses are replaced with a default value
    openai_descriptions = [
        desc if desc and isinstance(desc, str) else 'Description not available'
        for desc in openai_descriptions
    ]

    # Add the OpenAI descriptions to the DataFrame
    jobs_df['description'] = openai_descriptions
    return jobs_df

def create_combined_jobs_dataframe(handshake_jobs, indeed_jobs):
    # Find common columns
    common_columns = handshake_jobs.columns.intersection(indeed_jobs.columns)

    # Subset DataFrames to common columns and add source column
    handshake_jobs_common = handshake_jobs[common_columns]
    handshake_jobs_common['source'] = 'handshake'

    indeed_jobs_common = indeed_jobs[common_columns]
    indeed_jobs_common['source'] = 'indeed'

    jobs = pd.concat([handshake_jobs_common, indeed_jobs_common], ignore_index=True)
    return jobs

# Example usage
async def main():
    handshake_jobs = handshake_job_search('software engineer', 'fulltime')
    indeed_jobs = indeed_job_search('software engineer', 'fulltime')

    combined_jobs = create_combined_jobs_dataframe(handshake_jobs, indeed_jobs)
    updated_jobs = await add_description(combined_jobs)

    return updated_jobs

# Run the example usage
if __name__ == "__main__":
    asyncio.run(main())

import aiohttp
import asyncio
import random
import time
from bs4 import BeautifulSoup
from scrape_jobs import indeed_job_search, create_combined_jobs_dataframe, handshake_job_search, add_description
from flask_sqlalchemy import SQLAlchemy
from flask import Flask, request, jsonify, render_template
from flask import Flask, request, jsonify, render_template, redirect, url_for, session
from flask_sqlalchemy import SQLAlchemy
from werkzeug.security import generate_password_hash, check_password_hash
import logging
import numpy as np
import pandas as pd
import datetime
from authlib.integrations.flask_client import OAuth
import uuid
import os
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env file

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

app = Flask(__name__)


app.secret_key = os.getenv("FLASK_SECRET_KEY")

# Database Configuration
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('SQLALCHEMY_DATABASE_URI')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)


oauth = OAuth(app)
oauth.register(
    name='google',
    client_id=os.getenv('OAUTH_CLIENT_ID'),
    client_secret=os.getenv('OAUTH_CLIENT_SECRET'),
    server_metadata_url=os.getenv('OAUTH_META_URL'),
    scope='openid email profile',
)

class Company(db.Model):
    __tablename__ = 'companies'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, unique=True, nullable=False)
    alumni = db.relationship("Alumnus", back_populates="company")

class Alumnus(db.Model):
    __tablename__ = 'alumni'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'))
    company = db.relationship("Company", back_populates="alumni")
    school = db.Column(db.String, nullable=False)
    link = db.Column(db.String)

class User(db.Model):
    __tablename__ = 'users'
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String, unique=True, nullable=False)
    name = db.Column(db.String, nullable=False)
    school = db.Column(db.String, nullable=False)
    connections = db.Column(db.Integer, default=0, nullable=False)



class Job(db.Model):
    __tablename__ = 'jobs'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String, nullable=False)
    company_name = db.Column(db.String, nullable=False)
    description = db.Column(db.Text)
    job_url_direct = db.Column(db.String)
    source = db.Column(db.String)
    created_at = db.Column(db.DateTime, nullable=False, default=db.func.now())
    updated_at = db.Column(db.DateTime, nullable=False, default=db.func.now(), onupdate=db.func.now())
    search_id = db.Column(db.String, default=str(uuid.uuid4()), nullable=False)


class CompanySearch(db.Model):
    __tablename__ = 'company_searches'
    id = db.Column(db.Integer, primary_key=True)
    company_id = db.Column(db.Integer, db.ForeignKey('companies.id'), nullable=False)
    school = db.Column(db.String, nullable=False)
    last_searched = db.Column(db.DateTime, nullable=False, default=db.func.now())
    company = db.relationship("Company")

class Feedback(db.Model):
    __tablename__ = 'feedback'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey('users.id'), nullable=True)
    content = db.Column(db.Text, nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=db.func.now())


proxies_list = [
    {
        'username': 'brd-customer-hl_bec3d4fa-zone-serp_api3',
        'password': 'kwqy0vem0wj5',
        'port': 22225
    },
    {
        'username': 'brd-customer-hl_bec3d4fa-zone-serp_api2',
        'password': 'cls4c2ewcyj2',
        'port': 22225
    }
]



@app.route('/login')
def google_login():
    return oauth.google.authorize_redirect(redirect_uri=url_for("googleCallback", _external=True))

@app.route('/google-login')
def googleCallback():
    token = oauth.google.authorize_access_token()
    user_info = token.get('userinfo')
    print(user_info)
    if user_info:
        email = user_info['email']
        name = user_info['given_name']
        # Check if the user already exists
        user = User.query.filter_by(email=email).first()
        
        if not user:
            # If user does not exist, create a new user
            user = User(email=email, school="Unknown", name=name, connections=0)
            db.session.add(user)
            db.session.commit()
            logger.info(f"Created new user: {user}")
        else:
            logger.info(f"Existing user: {user}")
        
        # Save user info in the session
        session["user"] = {
            "email": email,
            "id": user.id,
            "connections": user.connections
        }
    
    return redirect(url_for("index"))



@app.route('/check_session')
def check_session():
    user_info = session.get("user")
    if not user_info:
        return jsonify({"logged_in": False})

    email = user_info.get("email")
    user = User.query.filter_by(email=email).first()
    
    if not user:
        # Create user if not exists in the database
        name = user_info.get("name", "Unknown")
        user = User(email=email, school="Unknown", name=name, connections=0)
        db.session.add(user)
        db.session.commit()
        logger.info(f"Created new user: {user}")

        # Update session with user ID
        session["user"]["id"] = user.id
    
    return jsonify({"logged_in": True})


@app.route('/logout')
def logout():
    session.pop("user", None)
    return redirect(url_for("index"))

def get_search_term(company_name, school_name):
    return f'site:linkedin.com ("{school_name}") ("{company_name}")'

def get_valid_proxies():
    valid_proxies = []
    for proxy in proxies_list:
        proxy_url = f"http://{proxy['username']}-session-{random.random()}:{proxy['password']}@brd.superproxy.io:{proxy['port']}"
        valid_proxies.append(proxy_url)
    return valid_proxies

async def fetch(session, url, proxy):
    try:
        async with session.get(url, proxy=proxy, ssl=False) as response:
            return await response.text()
    except Exception as e:
        logger.error(f"Error fetching {url}: {e}")
        return None

async def google_search(query, proxies):
    url = f"https://www.google.com/search?q={query}"
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    async with aiohttp.ClientSession(headers=headers) as session:
        tasks = [fetch(session, url, proxies[0])]
        for future in asyncio.as_completed(tasks):
            response = await future
            if response:
                return response
    return None

def extract_individuals(search_results, company_name, school_name):
    logger.info(f"Extracting individuals for {company_name}")
    start_time = time.time()
    verified_individuals = []
    soup = BeautifulSoup(search_results, 'html.parser')

    results = soup.find_all("div", class_="tF2Cxc")
    for result in results:
        
        description_elem = result.find("div", class_="LEwnzc Sqrs4e")
        if description_elem:
            title_elem = result.find("h3")
            link_elem = result.find("a")
            description_full_elem = result.find("div", class_="VwiC3b yXK7lf lVm3ye r025kc hJNv6b Hdw6tb")

            title = title_elem.text if title_elem else "No title"
            link = link_elem["href"] if link_elem else "No link"

            if description_full_elem:
                description_full_text = " ".join(span.get_text(separator=" ", strip=True) for span in description_full_elem.find_all("span"))
            else:
                description_full_text = ""
            if description_elem:
                description_text = " ".join(span.get_text(separator=" ", strip=True) for span in description_elem.find_all("span"))
            else:
                description_text = ""
            
            if "linkedin.com/in" in link and company_name.lower() in description_text.lower() and f"Education: {school_name}" in description_full_text:
                verified_individuals.append({
                    "title": title,
                    "link": link,
                    "description": description_text
                })

    logger.info(f"Number of individuals found for {company_name}: {len(verified_individuals)}")

    end_time = time.time()
    return verified_individuals

@app.route("/")
def index():
    return render_template("index.html", session=session.get("user"))   

@app.route("/results", methods=["GET"])
def results():
    return render_template("results.html")

def combine_jobs(existing_jobs_df, new_jobs):
    """
    Combines existing jobs DataFrame with new jobs DataFrame and adds openai_descriptions.
    
    Args:
    existing_jobs_df (pd.DataFrame): DataFrame of existing jobs.
    new_jobs (pd.DataFrame): DataFrame of new jobs.
    
    Returns:
    pd.DataFrame: Combined DataFrame with openai_descriptions.
    """
    combined_jobs = pd.concat([existing_jobs_df, new_jobs], ignore_index=True)
    combined_jobs = combined_jobs.reset_index(drop=True)
    return combined_jobs


@app.route("/search_jobs", methods=["POST"])
async def search_jobs_endpoint():
    data = request.get_json()
    search_term = data.get("search_term")
    job_type = data.get("job_type", "internship").lower()
    school_name = data.get("school_name", "Colgate University")
    current_time = datetime.datetime.now()
    search_id = str(uuid.uuid4())

    try:
        handshake_jobs = handshake_job_search(search_term, job_type)
        indeed_jobs = indeed_job_search(search_term, job_type)

        combined_jobs = create_combined_jobs_dataframe(handshake_jobs, indeed_jobs)
        combined_jobs.replace([np.inf, -np.inf], np.nan, inplace=True)
        combined_jobs.fillna("", inplace=True)

        existing_jobs = Job.query.with_entities(Job.title, Job.company_name).all()
        existing_jobs_set = {(job.title, job.company_name) for job in existing_jobs}

        new_jobs = combined_jobs[~combined_jobs.apply(lambda x: (x['title'], x['company']) in existing_jobs_set, axis=1)]
        existing_jobs_df = combined_jobs[combined_jobs.apply(lambda x: (x['title'], x['company']) in existing_jobs_set, axis=1)]

        new_jobs = await add_description(new_jobs, delay=0.5)

        for _, row in new_jobs.iterrows():
            job = Job(
                title=row['title'],
                company_name=row['company'],
                description=row['description'],
                job_url_direct=row['job_url_direct'],
                source=row['source'],
                created_at=current_time,
                updated_at=current_time,
                search_id=search_id
            )
            db.session.add(job)

        for _, row in existing_jobs_df.iterrows():
            job = Job.query.filter_by(title=row['title'], company_name=row['company']).first()
            if job:
                job.created_at = current_time
                job.updated_at = current_time
                job.search_id = search_id
                db.session.add(job)

        db.session.commit()

        combined_jobs = combine_jobs(existing_jobs_df, new_jobs)
        combined_jobs_dict = combined_jobs.to_dict(orient="records")

        return jsonify({"jobs": combined_jobs_dict, "search_id": search_id})
    except Exception as e:
        logger.error(f"Error in job search endpoint: {e}", exc_info=True)
        return jsonify({"detail": f"An error occurred while searching for jobs: {str(e)}"}), 500





@app.route("/search_jobs_and_individuals", methods=["POST"])
async def search_jobs_and_individuals_endpoint():
    data = request.get_json()
    search_term = data.get("search_term")
    school_name = data.get("school_name")
    
    try:
        start_request_time = time.time()

         # Get the most recent jobs based on updated_at
        jobs = Job.query.order_by(Job.updated_at.desc()).limit(100).all()
        
        jobs_df = pd.DataFrame([{
            'title': job.title,
            'company': job.company_name,
            'description': job.description,
            'job_url_direct': job.job_url_direct,
            'source': job.source,
        } for job in jobs])
      
        logger.info(f"Number of jobs retrieved: {len(jobs_df.index)}")

        companies = jobs_df['company'].unique().tolist()
        logger.info(f"Number of unique companies: {len(companies)}")
        
        alumni_batch_query = db.session.query(Alumnus).join(Company).filter(
            Company.name.in_(companies),
            Alumnus.school == school_name
        ).all()
        
        alumni_dict = {}
        for alumnus in alumni_batch_query:
            if alumnus.company.name not in alumni_dict:
                alumni_dict[alumnus.company.name] = []
            alumni_dict[alumnus.company.name].append({
                "name": alumnus.name,
                "link": alumnus.link,
                "description": ""
            })
        
        result_dict = {}
        for company in companies:
            job_details = jobs_df[jobs_df['company'] == company].to_dict(orient='records')
            result_dict[company] = {
                "job": job_details,
                "alumni": alumni_dict.get(company, [])
            }
            logger.info(f"Alumni found for {company} at {school_name} in the database: {len(alumni_dict.get(company, []))}")
        
        tasks = []
        proxies = get_valid_proxies()

        # Fetch all existing Company objects
        company_objs = {company.name: company for company in db.session.query(Company).filter(Company.name.in_(companies)).all()}
        
        # Create any missing Company objects
        new_companies = [Company(name=company) for company in companies if company not in company_objs]
        logger.info(f"Number of new companies to add: {len(new_companies)}")
        db.session.bulk_save_objects(new_companies)
        db.session.commit()
        company_objs.update({company.name: company for company in db.session.query(Company).filter(Company.name.in_(companies)).all()})

        # Fetch all existing CompanySearch objects for the given companies and school
        company_searches = db.session.query(CompanySearch).filter(
            CompanySearch.company_id.in_([company_objs[company].id for company in companies]),
            CompanySearch.school == school_name
        ).all()
        
        searched_companies = {search.company_id for search in company_searches}
        
        logger.info(f"Initial Searched companies: {searched_companies}")

        # Add tasks only for companies that have not been searched for the given school
        for company in companies:
            company_obj = company_objs[company]
            logger.info(f"Checking company: {company}, ID: {company_obj.id}, Searched: {company_obj.id in searched_companies}")
            if company_obj.id not in searched_companies:
                logger.info(f'This company has not been searched: {company_obj.id}')
                search_term = get_search_term(company, school_name)
                tasks.append(asyncio.ensure_future(google_search(search_term, proxies)))

        if tasks:
            responses = await asyncio.gather(*tasks)

            new_alumni = []
            new_company_searches = []
            for company, search_results in zip([c for c in companies if company_objs[c].id not in searched_companies], responses):
                if search_results:
                    print(company_objs[company].id)
                    verified_individuals = extract_individuals(search_results, company, school_name)
                    if verified_individuals:
                        result_dict[company]['alumni'].extend(verified_individuals)
                        
                        for individual in verified_individuals:
                            alumnus = Alumnus(
                                name=individual["title"],
                                company_id=company_objs[company].id,
                                school=school_name,
                                link=individual["link"]
                            )
                            new_alumni.append(alumnus)
                company_search = CompanySearch(
                            company_id=company_objs[company].id, 
                            school=school_name, 
                            last_searched=datetime.datetime.now()
                        )
                new_company_searches.append(company_search)        
                        
            
            if new_alumni:
                logger.info(f"New alumni to add: {len(new_alumni)}")
                db.session.bulk_save_objects(new_alumni)
            if new_company_searches:
                logger.info(f"New company searches to add: {len(new_company_searches)}")
                for search in new_company_searches:
                    logger.info(f"Adding search record for company_id: {search.company_id}, school: {search.school}")
                db.session.bulk_save_objects(new_company_searches)
            db.session.commit()

            # Logging the final state after commit
            company_searches = db.session.query(CompanySearch).filter(
                CompanySearch.company_id.in_([company_objs[company].id for company in companies]),
                CompanySearch.school == school_name
            ).all()
            searched_companies = {search.company_id for search in company_searches}
            logger.info(f"Final Searched companies after commit: {searched_companies}")

        function_end_time = time.time()
        logger.info(f"Time taken to retrieve individuals: {function_end_time - start_request_time} seconds")
        return jsonify(result_dict)
    except Exception as e:
        logger.error(f"Error in search jobs and individuals endpoint: {e}", exc_info=True)
        return jsonify({"detail": f"An error occurred while searching for individuals: {str(e)}"}), 500


@app.route("/update_connections", methods=["POST"])
def update_connections():
    try:
        user_id = session.get("user").get("id")
    except AttributeError as e:
        logger.error(f"Session attribute error: {e}")
        return jsonify({"error": "User not logged in or session invalid"}), 401

    if not user_id:
        logger.error("User ID not found in session")
        return jsonify({"error": "User not logged in"}), 401

    try:
        user = User.query.get(user_id)
        if user is None:
            logger.error(f"No user found with ID: {user_id}")
            return jsonify({"error": "User not found"}), 404
        
        user.connections += 1
        db.session.commit()

        total_users = User.query.count()
        users_with_more_connections = User.query.filter(User.connections > user.connections).count()
        percentile = ((total_users - users_with_more_connections) / total_users) * 100
        rounded_percentile = np.ceil(percentile / 0.005) * 0.005

        return jsonify({"connections": user.connections, "percentile": rounded_percentile, "name": user.email})
    except Exception as e:
        logger.error(f"Error updating connections: {e}", exc_info=True)
        return jsonify({"error": "An error occurred while updating connections"}), 500


@app.route("/create_first_dashboard", methods=["POST"])
def create_first_dashboard():
    try:
        user_id = session.get("user").get("id")
    except AttributeError as e:
        logger.error(f"Session attribute error: {e}")
        return jsonify({"error": "User not logged in or session invalid"}), 401

    if not user_id:
        logger.error("User ID not found in session")
        return jsonify({"error": "User not logged in"}), 401

    try:
        user = User.query.get(user_id)
        if user is None:
            logger.error(f"No user found with ID: {user_id}")
            return jsonify({"error": "User not found"}), 404
        
        logger.info(f"Creating first dashboard for user: {user.connections} connections")
        db.session.commit()

        total_users = User.query.count()
        users_with_more_connections = User.query.filter(User.connections > user.connections).count()
        percentile = ((total_users - users_with_more_connections) / total_users) * 100
        rounded_percentile = np.ceil(percentile / 0.005) * 0.005

        return jsonify({"connections": user.connections, "percentile": rounded_percentile, "name": user.email})
    except Exception as e:
        logger.error(f"Error updating connections: {e}", exc_info=True)
        return jsonify({"error": "An error occurred while updating connections"}), 500

@app.route("/submit_feedback", methods=["POST"])
def submit_feedback():
    data = request.get_json()
    content = data.get("content")
    
    user_id = session.get("user", {}).get("id")
    
    if not content:
        return jsonify({"error": "Feedback content is required"}), 400
    
    feedback = Feedback(user_id=user_id, content=content)
    db.session.add(feedback)
    db.session.commit()
    
    return jsonify({"message": "Feedback submitted successfully"})

@app.route("/feedback")
def feedback():
    return render_template("feedback.html")



def do_main():
    """ with app.app_context():
        db.drop_all()
        db.create_all() """
    app.run(debug=True, port=5000)

#do_main()
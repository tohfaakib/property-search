import urllib
from pathlib import Path
import logging
from importlib import import_module
from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI, Form, Request, BackgroundTasks, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from typing import Optional
from datetime import datetime

from pymongo import MongoClient
from pydantic import BaseModel
from bson import ObjectId


class SearchData(BaseModel):
    email: str
    search_id: str
    zipcode: str
    for_rent: bool
    is_all_homes: bool
    price_min: Optional[int]  # Make these fields optional
    price_max: Optional[int]
    monthly_payment_min: Optional[int]
    monthly_payment_max: Optional[int]
    monthly_cost_payment_min: Optional[int]
    monthly_cost_payment_max: Optional[int]
    is_coming_soon: bool
    is_auction: bool
    is_new_construction: bool
    list_price_active: bool
    is_townhouse: bool
    is_multi_family: bool
    is_condo: bool
    is_lot_land: bool
    is_apartment: bool
    is_manufactured: bool
    is_apartment_or_condo: bool
    max_hoa: Optional[int]
    beds: Optional[int]
    baths: Optional[float]
    run_every_weeks: int
    active_months: int
    created_at: datetime = datetime.now()


password = urllib.parse.quote_plus('@ddrr355v3rify')
mongo_uri = f"mongodb+srv://tohfaakib:{password}@cluster0.jscher1.mongodb.net/?retryWrites=true&w=majority&appName=AtlasApp"

# Create a MongoDB client
client = MongoClient(mongo_uri)

# Get a reference to the database
db = client.myDatabase

# Get a reference to the collection where you want to save search data
search_collection = db.searches

app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

output_dir = Path("output")
output_dir.mkdir(parents=True, exist_ok=True)

templates = Jinja2Templates(directory="templates")


@app.get("/")
def read_root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/files/", response_class=HTMLResponse)
async def list_files(request: Request):
    files = [str(file.name) for file in output_dir.glob("*.csv")]
    # files = [str(file.name) for file in output_dir.glob("*")]
    return templates.TemplateResponse("files.html", {"request": request, "files": files})


@app.get("/files/{file_name}")
async def download_file(file_name: str):
    file_path = output_dir / file_name
    if file_path.is_file():
        return FileResponse(file_path, filename=file_name)
    else:
        raise HTTPException(status_code=404, detail="File not found")


@app.get("/delete/{file_name}")
async def delete_file(file_name: str):
    file_path = output_dir / file_name
    if file_path.is_file():
        file_path.unlink()
        return RedirectResponse(url="/files")
    else:
        raise HTTPException(status_code=404, detail="File not found")


@app.get("/saved_searches/")
async def show_searches(request: Request):
    # Retrieve saved search data from MongoDB
    searches = list(search_collection.find({}))
    searches.reverse()

    # Render the HTML template with the search data
    return templates.TemplateResponse("searches.html", {"request": request, "searches": searches})


@app.post("/delete_search/{search_id}/")
async def delete_search(search_id: str):
    # Delete the search with the specified search_id
    search_collection.delete_one({"search_id": search_id})
    response = RedirectResponse(url="/saved_searches/")
    response.status_code = 302

    # Redirect back to the page displaying saved searches
    return response


@app.post("/run_script/")
async def run_script(request: Request, background_tasks: BackgroundTasks, zipcode: str = Form(...),
                     for_rent: bool = Form(False), is_all_homes: bool = Form(True),
                     price_min: Optional[int] = Form(None), price_max: Optional[int] = Form(None),
                     monthly_payment_min: Optional[int] = Form(None), monthly_payment_max: Optional[int] = Form(None),
                     monthly_cost_payment_min: Optional[int] = Form(None),
                     monthly_cost_payment_max: Optional[int] = Form(None),
                     is_coming_soon: bool = Form(False), is_auction: bool = Form(False),
                     is_new_construction: bool = Form(False), list_price_active: bool = Form(False),
                     is_townhouse: bool = Form(False), is_multi_family: bool = Form(False),
                     is_condo: bool = Form(False), is_lot_land: bool = Form(False),
                     is_apartment: bool = Form(False), is_manufactured: bool = Form(False),
                     is_apartment_or_condo: bool = Form(False), max_hoa: Optional[int] = Form(None),
                     beds: Optional[int] = Form(None), baths: Optional[float] = Form(None),
                     run_every_weeks: int = Form(...), active_months: int = Form(...),
                     email: str = Form(...)):
    # Generate a unique search_id (you can use ObjectId or UUID)
    search_id = str(ObjectId())  # Generate a unique ObjectId as search_id

    # Create a SearchData instance
    search_data = SearchData(
        email=email,
        search_id=search_id,
        zipcode=zipcode,
        for_rent=for_rent,
        is_all_homes=is_all_homes,
        price_min=price_min,
        price_max=price_max,
        monthly_payment_min=monthly_payment_min,
        monthly_payment_max=monthly_payment_max,
        monthly_cost_payment_min=monthly_cost_payment_min,
        monthly_cost_payment_max=monthly_cost_payment_max,
        is_coming_soon=is_coming_soon,
        is_auction=is_auction,
        is_new_construction=is_new_construction,
        list_price_active=list_price_active,
        is_townhouse=is_townhouse,
        is_multi_family=is_multi_family,
        is_condo=is_condo,
        is_lot_land=is_lot_land,
        is_apartment=is_apartment,
        is_manufactured=is_manufactured,
        is_apartment_or_condo=is_apartment_or_condo,
        max_hoa=max_hoa,
        beds=beds,
        baths=baths,
        run_every_weeks=run_every_weeks,
        active_months=active_months,
    )

    # Convert SearchData instance to a dictionary
    search_dict = search_data.dict()

    # Insert the search data into the MongoDB collection
    result = search_collection.insert_one(search_dict)

    print({"message": "Search data saved successfully", "search_id": search_id, "result": str(result)})

    try:
        crawler_module = import_module("zillow")
        start_parse = crawler_module.start_parse
    except (ImportError, AttributeError) as e:
        logging.error(f"Failed to import zillow.start_parse: {e}")
        raise HTTPException(status_code=500, detail="Failed to import crawler")

    background_tasks.add_task(start_parse, zipcode, for_rent, is_all_homes,
                              price_min, price_max, monthly_payment_min,
                              monthly_payment_max,
                              monthly_cost_payment_min,
                              monthly_cost_payment_max, is_coming_soon,
                              is_auction, is_new_construction,
                              list_price_active, is_townhouse,
                              is_multi_family, is_condo, is_lot_land,
                              is_apartment, is_manufactured,
                              is_apartment_or_condo, max_hoa, beds, baths, email)

    result_message = "Scraper is running in the background"

    return templates.TemplateResponse("result.html", {"request": request, "result_message": result_message})


# Create a background scheduler

from datetime import datetime


def run_scheduled_task():
    # Get the current week number (0-51)
    current_week = datetime.now().isocalendar()[1]

    # Retrieve all saved searches from MongoDB
    searches = list(search_collection.find({}))

    for search in searches:
        try:
            weeks_since_creation = (datetime.now() - search["created_at"]).days // 7

            if search["run_every_weeks"] <= 0:
                continue

            # Check if it's time to run the task based on run_every_weeks
            if weeks_since_creation % search["run_every_weeks"] == 0:
                # Check if the task is still active based on active_months
                if (datetime.now() - search["created_at"]).days <= search["active_months"] * 30:
                    try:
                        crawler_module = import_module("zillow")
                        start_parse = crawler_module.start_parse
                    except (ImportError, AttributeError) as e:
                        logging.error(f"Failed to import zillow.start_parse: {e}")
                        continue

                    # Call start_parse with the search data
                    start_parse(
                        search["zipcode"], search["for_rent"], search["is_all_homes"],
                        search["price_min"], search["price_max"], search["monthly_payment_min"],
                        search["monthly_payment_max"], search["monthly_cost_payment_min"],
                        search["monthly_cost_payment_max"], search["is_coming_soon"],
                        search["is_auction"], search["is_new_construction"],
                        search["list_price_active"], search["is_townhouse"],
                        search["is_multi_family"], search["is_condo"], search["is_lot_land"],
                        search["is_apartment"], search["is_manufactured"],
                        search["is_apartment_or_condo"], search["max_hoa"], search["beds"], search["baths"], search["email"]
                    )

                    # Mark the search as "run for the specific week"
                    search_collection.update_one({"_id": search["_id"]}, {"$set": {"last_run_week": current_week}})

        except Exception as e:
            logging.error(f"Failed to run scheduled task for search_id: {search['search_id']}: {e}")
            continue


scheduler = BackgroundScheduler()
scheduler.start()
# Schedule the task using a cron-like expression
scheduler.add_job(run_scheduled_task, CronTrigger(day_of_week='mon', hour=0, minute=0))

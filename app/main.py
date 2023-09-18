from pathlib import Path
import logging
from importlib import import_module
from fastapi.staticfiles import StaticFiles
from fastapi import FastAPI, Form, Request, BackgroundTasks, HTTPException
from fastapi.templating import Jinja2Templates
from fastapi.responses import FileResponse, HTMLResponse, RedirectResponse
from typing import Optional


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
    files = [str(file.name) for file in output_dir.glob("*")]
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
        # return {"message": "File deleted"}
        # redirect to files page
        return RedirectResponse(url="/files")
    else:
        raise HTTPException(status_code=404, detail="File not found")


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
                     beds: Optional[int] = Form(None), baths: Optional[float] = Form(None)):
    # module_path = "/app/"
    # sys.path.append(module_path)
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
                              is_apartment_or_condo, max_hoa, beds, baths)

    result_message = "Scraper is running in the background"

    return templates.TemplateResponse("result.html", {"request": request, "result_message": result_message})

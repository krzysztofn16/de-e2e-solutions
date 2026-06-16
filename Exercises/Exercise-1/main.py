from pathlib import Path
import shutil
from urllib.parse import urlparse
import requests
import zipfile
from tqdm import tqdm
import logging
import time
import asyncio
import aiohttp
import click

logging.basicConfig(
    level=logging.INFO,
    format = "%(asctime)s - %(levelname)s - %(message)s",
)

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",
]

def create_download_dir(download_dir: Path) -> None:
    download_dir.mkdir(parents=True, exist_ok=True)
    if download_dir.exists():
        logging.info("Directory created: %s",download_dir)

def get_filename_from_url(file_url: str) -> str:
    # return file_url.split('/')[1]
    return Path(urlparse(file_url).path).name

def download_file(file_url: str, download_dir: Path, session: requests.Session) -> Path | None:
    filename = get_filename_from_url(file_url)
    download_path = download_dir / filename
    csv_path = download_path.with_suffix(".csv")
    
    if csv_path.exists():
        logging.info("CSV file already exists: %s, skipping download and extraction", csv_path)
        return None
    
    if download_path.exists():
        logging.info("File already exists: %s, skipping download", download_path)
        return download_path
    
    # maybe not necessery but why not add for learnign purposes, here I can configure headers for multiple requests if needed
    try:
        response = session.get(file_url, timeout=10)
        response.raise_for_status()
    except requests.exceptions.RequestException as error:
        logging.exception("Failed to download file from url: %s, error: %s", file_url, error)
        return None

    download_path.write_bytes(response.content)
    logging.info("File downloaded: %s, size: %d bytes, status_code: %d", download_path, download_path.stat().st_size, response.status_code)

    return download_path

async def download_file_async(
    file_url: str,
    download_dir: Path, 
    session: aiohttp.ClientSession,
    semaphore: asyncio.Semaphore,
) -> tuple[Path, int] | tuple[None,int] | None:
    filename = get_filename_from_url(file_url)
    download_path = download_dir / filename
    csv_path = download_path.with_suffix(".csv")
    
    if csv_path.exists():
        logging.info("CSV file already exists: %s, skipping download and extraction", csv_path)
        return None, 1000
    
    if download_path.exists():
        logging.info("File already exists: %s, skipping download", download_path)
        return download_path, 1000
    
    # maybe not necessery but why not add for learnign purposes, here I can configure headers for multiple requests if needed
    async with semaphore:
        try:
            async with session.get(file_url) as response:
                response.raise_for_status()
                content = await response.read()
                download_path.write_bytes(content)

                logging.info(
                    "File downloaded: %s, size: %d bytes, status_code: %d", 
                    download_path, 
                    download_path.stat().st_size, 
                    response.status,
                )
        except aiohttp.ClientResponseError as error:
            logging.error(
                "HTTP error while downloading url: %s status: %s, message: %s",
                file_url,
                error.status,
                error.message,
            )
            return None, error.status
        except asyncio.TimeoutError:
            logging.error("Timeout while downloading url: %s", file_url)
            return None, error.status
        except aiohttp.ClientError as error:
            logging.exception("Failed to download file from url: %s, error: %s", file_url, error)
            return None, error.status

    return download_path, response.status

async def download_file_async_tqdm(
    file_url: str,
    download_dir: Path,
    session: aiohttp.ClientSession,
    progress_bar: tqdm,
) -> tuple[Path, int] | tuple[None,int] | None:
    filename = get_filename_from_url(file_url)
    download_path = download_dir / filename
    csv_path = download_path.with_suffix(".csv")

    if csv_path.exists():
        logging.info("CSV file already exists: %s, skipping download and extraction", csv_path)
        return None,1000

    if download_path.exists():
        logging.info("ZIP file already exists: %s, skipping download", download_path)
        return download_path, 1000

    try:
        async with session.get(file_url) as response:
            response.raise_for_status()

            content_length = response.headers.get("Content-Length")

            if content_length is not None:
                progress_bar.total = (progress_bar.total or 0) + int(content_length)
                progress_bar.refresh()

            with open(download_path, "wb") as file:
                async for chunk in response.content.iter_chunked(64 * 1024):
                    file.write(chunk)
                    progress_bar.update(len(chunk))

    except aiohttp.ClientResponseError as error:
        logging.error(
            "HTTP error while downloading url: %s status: %s, message: %s",
            file_url,
            error.status,
            error.message,
        )
        return None, error.status

    except aiohttp.ClientError as error:
        logging.error("Failed to download file from url: %s, error: %s", file_url, error)
        return None, error.status

    except asyncio.TimeoutError:
        logging.error("Timeout while downloading url: %s", file_url)
        return None, error.status

    return download_path, response.status

def log_download_summary(results: list[tuple[Path|None,int]]) -> None:
    total = len(results)
    downloaded = sum(1 for path,status in results if (status - 200) < 100)
    skipped = sum(1 for path,status in results if status == 1000)
    failed = sum(1 for path,status in results if status - 400 < 100 and status - 400 > 0)

    logging.info(
        "Download stage finished: total %d, downloaded %d, skipped %d, failed %d",
        total,
        downloaded,
        skipped,
        failed,
    )

def extract_zip(downloaded_file_path: Path, download_dir: Path) -> None:
    if not downloaded_file_path.exists() or not zipfile.is_zipfile(downloaded_file_path):
        logging.warning("File does not exist or is not a zip file, skipping extraction")
        return
    
    try:
        with zipfile.ZipFile(downloaded_file_path,'r') as zip_ref:
            zip_ref.extractall(download_dir)
    except zipfile.BadZipFile:
        logging.exception("Failed to extract zip file: %s, file may be corrupted", downloaded_file_path)
        return
    
    if downloaded_file_path.exists():
        downloaded_file_path.unlink()

    logging.info("Extracted file %s and cleaned.", downloaded_file_path)

def clean_extracts(download_dir: Path) -> None:
    macosdir = download_dir / "__MACOSX"
    if macosdir.is_dir():
        shutil.rmtree(macosdir)

async def main(
        tqdm_print: bool,
        concurency_num: int,
        download_dir: Path,
) -> None:

    BASE_DIR = Path(__file__).resolve().parent
    download_dir = BASE_DIR / download_dir
    tqdm_print = tqdm_print

    create_download_dir(download_dir)
    timeout = aiohttp.ClientTimeout(total=60)

    if not tqdm_print:
        semaphore = asyncio.Semaphore(concurency_num)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            tasks = [
                download_file_async(file_url, download_dir, session, semaphore) 
                for file_url in download_uris
            ]

            downloaded_paths_status = await asyncio.gather(*tasks)
        
        log_download_summary(downloaded_paths_status)

        for downloaded_file_path, status in downloaded_paths_status:
            if downloaded_file_path is None:
                continue

            extract_zip(downloaded_file_path, download_dir)
    else:
        async with aiohttp.ClientSession(timeout=timeout) as session:
            with tqdm(
                total=0,
                desc="Downloading files",
                unit="B",
                unit_scale=True,
                unit_divisor=1024,
            ) as progress_bar:
                tasks = [
                    download_file_async_tqdm(file_url, download_dir, session, progress_bar)
                    for file_url in download_uris
                ]

                downloaded_paths_status = await asyncio.gather(*tasks)

        log_download_summary(downloaded_paths_status)
        
        for downloaded_file_path, status in downloaded_paths_status:
            if downloaded_file_path is None:
                continue

            extract_zip(downloaded_file_path, download_dir)

    clean_extracts(download_dir)

@click.command()
@click.option("--tqdm/--no-tqdm", default=True, help="Show download progress bar.")
@click.option("--concurrency", default=3, help="Number of concurent downloads.")
@click.option("--download-dir", default="downloads", help="Directory where files will be downloaded.")

def cli(tqdm:bool,concurrency:int,download_dir:str)->None:
    asyncio.run(main(tqdm, concurrency, Path(download_dir)))

if __name__ == "__main__":
    without_asyncio_time = 33.24#sec
    with_asyncio_time = 10.31
    with_asyncio_with_semaphore_time = 10.20
    start_time = time.time()
    cli()
    end_time = time.time()
    logging.info("Execution time: %.2f seconds", end_time - start_time)
    logging.info("Expected time with asyncio and semaphore: %.2f seconds", with_asyncio_with_semaphore_time)
    logging.info("Expected time with asyncio: %.2f seconds", with_asyncio_time)
    logging.info("Expected time without asyncio: %.2f seconds", without_asyncio_time)

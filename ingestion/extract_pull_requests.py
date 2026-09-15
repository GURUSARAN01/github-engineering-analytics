import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv

import boto3
from botocore.exceptions import (
    BotoCoreError,
    ClientError,
)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

load_dotenv()

GITHUB_TOKEN = os.getenv("GITHUB_TOKEN")
GITHUB_OWNER = os.getenv("GITHUB_OWNER", "dbt-labs")
GITHUB_REPO = os.getenv("GITHUB_REPO", "dbt-core")

API_VERSION = "2026-03-10"

BASE_URL = "https://api.github.com"

RAW_DIR = Path("data/raw/github")
STATE_DIR = Path("data/state")

WATERMARK_FILE = STATE_DIR / "pull_requests_watermark.json"

AWS_PROFILE = os.getenv(
    "AWS_PROFILE",
    "github-de",
)

AWS_REGION = os.getenv(
    "AWS_REGION",
    "eu-central-1",
)

S3_BUCKET = os.getenv("S3_BUCKET")

# ---------------------------------------------------------
# HTTP headers
# ---------------------------------------------------------

def build_headers()-> dict:
    headers = {
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": API_VERSION,
        "User-Agent": "github-engineering-analytics-project",
    }

    if GITHUB_TOKEN:
        headers["Authorization"] = f"Bearer {GITHUB_TOKEN}"

    return headers

# ---------------------------------------------------------
# Watermark handling
# ---------------------------------------------------------

def load_watermark() -> datetime | None:
    if not WATERMARK_FILE.exists():
        return None

    with WATERMARK_FILE.open("r", encoding="utf-8") as file:
        state = json.load(file)

    watermark_text = state.get("last_updated_at")

    if not watermark_text:
        return None

    return datetime.fromisoformat(
        watermark_text.replace("Z", "+00:00")
    )

def save_watermark(value: datetime) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)

    state = {
        "last_updated_at": value.astimezone(timezone.utc).isoformat()
    }

    with WATERMARK_FILE.open("w", encoding="utf-8") as file:
        json.dump(state, file, indent=2)

# ---------------------------------------------------------
# API extraction
# ---------------------------------------------------------

def fetch_pull_requests(
    owner: str,
    repo: str,
    watermark: datetime | None = None,
    max_pages: int | None = None,
) -> list[dict]:

    url = f"{BASE_URL}/repos/{owner}/{repo}/pulls"

    headers = build_headers()

    all_records: list[dict] = []

    page = 1

    while True:

        # Optional safety limit
        if max_pages is not None and page > max_pages:
            print(
                f"Reached configured page limit: {max_pages}"
            )
            break

        params = {
            "state": "all",
            "sort": "updated",
            "direction": "desc",
            "per_page": 100,
            "page": page,
        }

        print(f"Requesting page {page}...")

        response = requests.get(
            url,
            headers=headers,
            params=params,
            timeout=30,
        )

        print(
            "Rate limit remaining:",
            response.headers.get(
                "X-RateLimit-Remaining"
            ),
        )

        # -----------------------------------
        # Rate-limit handling
        # -----------------------------------

        if response.status_code in (403, 429):

            reset_time = response.headers.get(
                "X-RateLimit-Reset"
            )

            raise RuntimeError(
                f"GitHub rate limit reached. "
                f"Reset time: {reset_time}"
            )

        # Fail for 4xx/5xx errors
        response.raise_for_status()

        records = response.json()

        # -----------------------------------
        # No records = finished
        # -----------------------------------

        if not records:
            print("No more records.")
            break

        stop_pagination = False

        # -----------------------------------
        # Process page
        # -----------------------------------

        for record in records:

            updated_at = datetime.fromisoformat(
                record["updated_at"].replace(
                    "Z",
                    "+00:00",
                )
            )

            if (
                watermark is not None
                and updated_at <= watermark
            ):
                stop_pagination = True
                break

            all_records.append(record)

        # -----------------------------------
        # Hit previous watermark
        # -----------------------------------

        if stop_pagination:
            print(
                "Reached existing watermark. "
                "Stopping pagination."
            )
            break

        # -----------------------------------
        # Less than 100 means last page
        # -----------------------------------

        if len(records) < 100:
            print("Reached final page.")
            break

        # Move to next page
        page += 1

    return all_records

# ---------------------------------------------------------
# Raw storage
# ---------------------------------------------------------

def save_raw_json(records: list[dict], owner: str, repo: str,) -> Path:

    extraction_time = datetime.now(timezone.utc)

    date_path = extraction_time.strftime("%Y/%m/%d")

    destination = (RAW_DIR/f"repo={owner}-{repo}"/"entity=pull_requests"/date_path)

    destination.mkdir(parents=True, exist_ok=True)

    timestamp = extraction_time.strftime("%Y%m%dT%H%M%SZ")

    file_path = (destination/f"pull_requests_{timestamp}.json")

    payload = {
        "metadata": {
            "source": "github",
            "owner": owner,
            "repository": repo,
            "entity": "pull_requests",
            "extracted_at": extraction_time.isoformat(),
            "record_count": len(records),
            "api_version": API_VERSION,
        },
        "records": records,
    }

    with file_path.open("w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, ensure_ascii=False,)

    return file_path

# ---------------------------------------------------------
# Build S3 Client
# ---------------------------------------------------------

def build_s3_client():

    session = boto3.Session(
        profile_name=AWS_PROFILE,
        region_name=AWS_REGION,
    )

    return session.client("s3")

def build_s3_key(file_path: Path,) -> str:
    relative_path = file_path.relative_to(RAW_DIR)

    return("raw/github/" + relative_path.as_posix())


def upload_raw_file_to_s3(file_path: Path,) -> str:
    if not S3_BUCKET:
        raise ValueError("S3_BUCKET is not configured.")
    s3_client = build_s3_client()

    s3_key = build_s3_key(file_path)

    try:
        s3_client.upload_file(
            str(file_path),
            S3_BUCKET,
            s3_key,
            ExtraArgs={
                "ContentType":
                "application/json"
            },
        )

    except (ClientError, BotoCoreError,) as exec:
        raise RuntimeError(
            f"Failed to upload "
            f"{file_path} to S3"
        ) from exec

    return(f"s3://{S3_BUCKET}/{s3_key}")

    
# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    print(f"Repository: "
          f"{GITHUB_OWNER}/{GITHUB_REPO}")

    watermark = load_watermark()

    print("Current watermark:", watermark or "None - full initial extraction",)

    records = fetch_pull_requests(owner=GITHUB_OWNER, repo=GITHUB_REPO, watermark=watermark, max_pages=1)

    print(
        f"Extracted {len(records)} "
        f"new/updated pull requests."
    )

    if not records:
        print("Nothing new to save.")
        return

    file_path = save_raw_json(records, GITHUB_OWNER, GITHUB_REPO)

    print(f"Save raw data to: {file_path}")


    file_path = save_raw_json(records, GITHUB_OWNER, GITHUB_REPO,)

    print(f"Saved raw data to: {file_path}")

    s3_uri = upload_raw_file_to_s3(file_path)
    print(f"Uploaded raw data to: {s3_uri}")

    newest_updated_at = max(datetime.fromisoformat(
        record["updated_at"].replace(
            "Z", "+00:00"))
            for record in records
            )

    save_watermark(newest_updated_at)   
    print("New watermark: ", newest_updated_at)

if __name__ == "__main__":
    main()
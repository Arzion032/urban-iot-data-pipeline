import os
import uuid
import requests
import boto3
from dotenv import load_dotenv

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
dotenv_path = os.path.join(BASE_DIR, ".env")

# Load .env from the project root
load_dotenv(dotenv_path)

# S3 client
s3 = boto3.client(
    "s3",
    region_name=os.getenv("AWS_REGION")
)

def generate_and_upload_random_image():
    """
    Generates a random image (simulated traffic camera snapshot)
    and uploads it to S3. Returns the S3 URL.
    """
    # Step 1: Fetch random image from Picsum
    picsum_url = "https://picsum.photos/512"
    response = requests.get(picsum_url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch image: {response.status_code}")

    img_bytes = response.content

    # Step 2: Generate unique filename
    filename = f"traffic_{uuid.uuid4()}.jpg"

    # Step 3: Upload to S3
    bucket = os.getenv("BUCKET_NAME")
    s3.put_object(
        Bucket=bucket,
        Key=filename,
        Body=img_bytes,
        ContentType="image/jpeg"
    )

    # Step 4: Construct S3 URL (public bucket)
    region = os.getenv("AWS_REGION")
    url = f"https://{bucket}.s3.{region}.amazonaws.com/{filename}"
    return url

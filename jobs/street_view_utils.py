from dotenv import load_dotenv
from PIL import Image
import os
import uuid
import requests
import boto3
import io

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
    Includes fallback if the API request fails.
    """

    def fetch_image(url):
        try:
            response = requests.get(url, timeout=5)
            if response.status_code == 200:
                return response.content
        except Exception:
            return None
        return None

    # PRIMARY SOURCE — Picsum
    img_bytes = fetch_image("https://picsum.photos/512")

    # FALLBACK 1 — PlaceKitten
    if img_bytes is None:
        img_bytes = fetch_image("https://placekitten.com/512/512")

    # FALLBACK 2 — Guaranteed 1x1 pixel image
    if img_bytes is None:
        img = Image.new("RGB", (1, 1), color=(128, 128, 128))
        buffer = io.BytesIO()
        img.save(buffer, format="JPEG")
        img_bytes = buffer.getvalue()

    # Generate unique filename
    filename = f"traffic_{uuid.uuid4()}.jpg"

    # Upload to S3
    bucket = os.getenv("BUCKET_NAME")
    s3.put_object(
        Bucket=bucket,
        Key=filename,
        Body=img_bytes,
        ContentType="image/jpeg"
    )

    # Construct S3 URL
    region = os.getenv("AWS_REGION")
    url = f"https://{bucket}.s3.{region}.amazonaws.com/{filename}"
    return url
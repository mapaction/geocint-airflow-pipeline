import requests
import logging
import sys  # Import sys for command-line arguments

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def download_file(url, save_path):
  """Downloads a file from a URL and saves it to the specified path.

  Logs download attempts, success, and failures with additional information.

  Args:
    url: The URL of the file to download.
    save_path: The path where the file should be saved.
  """

  logging.info(f"Downloading file from: {url}")
  try:
    response = requests.get(url, stream=True)
    response.raise_for_status()  # Raise an exception for unsuccessful downloads

    # Extract filename from response headers (if available)
    filename = response.headers.get('Content-Disposition', None)
    if filename:
      filename = filename.split('=')[1].strip('"')
    else:
      filename = url.split('/')[-1]

    # Create the save path if it doesn't exist
    import os
    os.makedirs(os.path.dirname(save_path), exist_ok=True)  # Create directories if needed

    # Download the file in chunks for efficiency
    with open(save_path, "wb") as f:
      for chunk in response.iter_content(chunk_size=1024):
        if chunk:  # filter out keep-alive new chunks
          f.write(chunk)

    # Log success only if download completes without exceptions
    logging.info(f"Download successful: Downloaded '{filename}' to {save_path}")
  except requests.exceptions.RequestException as e:
    logging.error(f"Download failed: Error downloading {url} - {e}")


if __name__ == "__main__":
  if len(sys.argv) != 3:
    print("Usage: python script.py <url> <save_path>")
    sys.exit(1)

  url = sys.argv[1]
  save_path = sys.argv[2]

  download_file(url, save_path)
  print("Download attempted. Check logs for details.")

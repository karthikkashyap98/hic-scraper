# Hi-C Scrapers

A compilation of python scripts to scrape HiC files from different sources

## How to Run

The project is organized into directories based on the source the data is scraped or downloaded from.  
Each directory (e.g., `4dn`, `encode`, `geo`, `synapse`) contains scripts relevant to that source.

## Steps

**Navigate to the desired directory**

```bash
cd 4dn
```

**Set up a virtual environment**

If you are already in another virtual environment, deactivate it:

```bash
deactivate
```

Create a new virtual environment:

```bash
python3 -m venv env
```

Activate the environment:

```bash
source env/bin/activate
```

3. **Install dependencies**

```bash
pip install -r requirements.txt
```

4. **Run the required script**

- Use `scraper.py` for scraping data.
- Use `downloader.py` for downloading files (if available).

Some sources may have only one script (`scraper.py`), while others may have both.

Example:

```bash
python scraper.py
```

or

```bash
python downloader.py
```

# Portfolio Projects: SCOPUS & OpenAlex Data Pipeline

A Python ETL pipeline that extracts research publication data from the SCOPUS API (and potentially OpenAlex), transforms it, and loads it into a PostgreSQL database for analysis and visualization.

## 📌 Features

- Automated extraction of research publications and metadata
- Data cleaning and transformation using Pandas
- Secure credential management using environment variables
- Error handling and logging at each pipeline stage
- Modular design for scalability and maintainability

## Project Structure

scopus_openalex_projects/
├── project1.py  
├── requirements.txt  
├── .env  
├── .gitignore  
└── README.md

## ⚙️ Setup

### Prerequisites

- Python 3.8+
- PostgreSQL database (local hosted)
- SCOPUS API Access (requires an API key)

### Installation

1. Clone the repository:
   `git clone https://github.com/bennisyiu/ipao-portfolio-projects.git`
   `cd ipao-portfolio-projects`

2. Create a virtual environment:
   `python3 -m venv venv`
   `source venv/bin/activate` # Linux/Mac
   `.\venv\Scripts\activate` # Windows

3. Install dependencies:
   `pip install -r requirements.txt`
   or
   `pip3 install -r requirements.txt`

4. Set up environment variables:
   `SCOPUS_API_KEY=your_scopus_api_key`
   `DB_HOST=localhost`
   `DB_PORT=5432`
   `DB_USER=your_db_user`
   `DB_PASSWORD=your_db_password`
   `DB_NAME=your_db_name`

### Execution

1. in terminal: `python project1.py` # or 'python3' instead

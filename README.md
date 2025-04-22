# Portfolio Projects: SCOPUS & OpenAlex Data Pipeline

A Python ETL pipeline that extracts research publication data from the SCOPUS API, transforms it, and loads it into a PostgreSQL database for analysis and visualization.

## 📌 Features

- Automated extraction of research publications and metadata
- Data cleaning and transformation using Pandas
- Secure credential management using environment variables
- Error handling and logging at each pipeline stage
- Modular design for scalability and maintainability

## Project Structure

scopus_openalex_projects <br>
├── scopus_project.py  
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

1. Clone the repository: <br>
   `git clone https://github.com/bennisyiu/ipao-portfolio-projects.git`
   <br>
   `cd ipao-portfolio-projects`

2. Create a virtual environment: <br>
   `python3 -m venv venv`
   <br>
   `source venv/bin/activate` # Linux or Mac <br>

   `.\venv\Scripts\activate` # Windows

3. Install dependencies: <br>
   `pip install -r requirements.txt`

4. Set up environment variables: <br>

   Replace the following details with your own credentials <br>

   `SCOPUS_API_KEY=your_scopus_api_key` <br>
   `DB_HOST=localhost` <br>
   `DB_PORT=5432` <br>
   `DB_USER=your_db_user` <br>
   `DB_PASSWORD=your_db_password` <br>
   `DB_NAME=your_db_name` <br>

### Execution

1. in terminal: `python scopus_project.py`

# 🚀 Crypto ETL Pipeline
# 📖 Project Overview

The Crypto ETL Pipeline is an end-to-end Data Engineering project that extracts real-time cryptocurrency market data from the CoinGecko API, transforms the data using Python, and loads it into Amazon Redshift for analytics. The pipeline is orchestrated using Apache Airflow and deployed on AWS with an automated CI/CD pipeline using GitHub Actions.

# 🎯 Objectives
Extract cryptocurrency market data from CoinGecko API
Clean and transform raw data
Load processed data into Amazon Redshift
Automate ETL workflow using Apache Airflow
Implement CI/CD using GitHub Actions
Deploy the project on AWS EC2

# 🏗️ Project Architecture
CoinGecko API
      │
      ▼
Extract (Python)
      │
      ▼
Transform (Pandas)
      │
      ▼
Amazon Redshift
      │
      ▼
Analytics
      ▲
      │
Apache Airflow
      ▲
      │
GitHub Actions (CI/CD)

# 🛠️ Tech Stack
Python
Apache Airflow
Amazon EC2
Amazon Redshift
Amazon S3 
Git
GitHub
GitHub Actions
Docker
Pandas
Requests
Boto3

# 📂 Project Structure
crypto-etl-pipeline/
│
├── .github/
├── src/
├── config/
├── tests/
├── docs/
├── requirements.txt
├── README.md
└── setup.py

⚙️ Installation
git clone git@github.com:YOUR_USERNAME/crypto-etl-pipeline.git
cd crypto-etl-pipeline
pip install -r requirements.txt

▶️ Run Project
python main.py

🧪 Testing
pytest tests/

🚀 CI/CD

GitHub Actions automatically:

Run code quality checks
Execute unit tests
Deploy to AWS EC2
Update Airflow DAGs

☁️ AWS Services Used
Amazon EC2
Amazon Redshift
IAM
VPC
Security Groups
📈 Future Enhancements
Add data quality validation
Integrate Amazon S3
Add monitoring with CloudWatch
Implement Slack notifications
👨‍💻 Author

Diwakar K

# QueryMind — NL to SQL with Olist E-Commerce Data (Django)

> Ask questions in plain English about 100K+ real Brazilian e-commerce orders
## 📸 Screenshot
![NL2SQL Studio Demo](static/img/Screenshot%20(11).png )
![NL2SQL Studio Demo](static/img/Capture.png )
## Setup Guide (Step by Step)
### Step 1: Download Olist Dataset from Kaggle
1. Go to: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
2. Click "Download" (free Kaggle account needed)
3. Extract archive.zip — you get 9 CSV files (~50MB)

### Step 2: Setup Project
```bash
cd nl2sql_demo
python -m venv venv
venv\Scripts\activate          # Windows
pip install -r requirements.txt
```

### Step 3: Create Database Tables
```bash
python manage.py makemigrations warehouse
python manage.py migrate
```

### Step 4: Import CSV Data
```bash
python manage.py import_olist_data "C:\Users\YourName\Downloads\archive"
# To skip geolocation (1M rows): add --skip-geo flag
```

### Step 5: Run
```bash
python manage.py runserver
```
Open http://127.0.0.1:8000

## Claude API (Optional)
Without key = Demo mode (10 pre-built queries)
With key = Any English question works

```bash
set ANTHROPIC_API_KEY=sk-ant-xxxxx   # Windows
python manage.py runserver
```

## Sample Questions
- Monthly revenue trend
- Top product categories by sales
- Customer distribution by state
- Payment method breakdown
- Review score distribution
- Average delivery time by state
- Top seller cities


Questions like:
Products:

"How many products are there?"
"Product count by category"
"Show me heaviest products"

Orders:

"Total orders this year"
"How many orders were cancelled?"
"Average order value"

Revenue:

"Monthly revenue trend"
"Total revenue by state"
"Top 10 categories by sales"

Customers:

"How many unique customers?"
"Which city has most customers?"
"Customer count by state"



"Show me everything about payments"
"Which seller has most orders?"
"Average delivery time"
"5 star reviews kitne hain?"

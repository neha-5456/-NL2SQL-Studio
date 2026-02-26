"""
NL2SQL Engine for Olist Brazilian E-Commerce Dataset.

Flow:
1. User asks a question in plain English
2. We send the question + Olist schema to Claude
3. Claude generates SQL
4. We validate the SQL (SELECT only, no dangerous ops)
5. We execute the SQL on SQLite
6. If error → send error back to Claude for self-correction
7. Return clean results
"""

import re
import json
import time
import sqlite3
import logging
from decimal import Decimal
from datetime import datetime, date
from django.conf import settings

logger = logging.getLogger(__name__)


# ============================================
# 1. OLIST DATABASE SCHEMA
# ============================================
DATABASE_SCHEMA = """
## Database: Olist Brazilian E-Commerce (SQLite)
## 100K+ real orders from 2016-2018, Brazilian marketplace
## Currency: BRL (Brazilian Real, R$)

### Table: olist_customers
| Column                     | Type        | Description                                         |
|----------------------------|-------------|-----------------------------------------------------|
| customer_id                | VARCHAR(50) PK | Unique ID for each order-customer mapping        |
| customer_unique_id         | VARCHAR(50) | Unique customer (same person across orders)         |
| customer_zip_code_prefix   | VARCHAR(10) | First 5 digits of customer zip code                 |
| customer_city              | VARCHAR(100)| Customer city name                                  |
| customer_state             | VARCHAR(5)  | Customer state abbreviation (SP, RJ, MG, etc.)      |

### Table: olist_sellers
| Column                   | Type        | Description                                |
|--------------------------|-------------|--------------------------------------------|
| seller_id                | VARCHAR(50) PK | Unique seller ID                        |
| seller_zip_code_prefix   | VARCHAR(10) | First 5 digits of seller zip code          |
| seller_city              | VARCHAR(100)| Seller city name                           |
| seller_state             | VARCHAR(5)  | Seller state abbreviation                  |

### Table: olist_products
| Column                     | Type        | Description                                |
|----------------------------|-------------|--------------------------------------------|
| product_id                 | VARCHAR(50) PK | Unique product ID                       |
| product_category_name      | VARCHAR(100)| Category in Portuguese (join with translation table)|
| product_name_length        | INTEGER     | Number of characters in product name       |
| product_description_length | INTEGER     | Number of characters in description        |
| product_photos_qty         | INTEGER     | Number of product photos                   |
| product_weight_g           | INTEGER     | Product weight in grams                    |
| product_length_cm          | INTEGER     | Product length in cm                       |
| product_height_cm          | INTEGER     | Product height in cm                       |
| product_width_cm           | INTEGER     | Product width in cm                        |

### Table: product_category_translation
| Column                          | Type        | Description                         |
|---------------------------------|-------------|-------------------------------------|
| product_category_name           | VARCHAR(100) PK | Portuguese category name         |
| product_category_name_english   | VARCHAR(100)| English translation of category     |

### Table: olist_orders
| Column                        | Type       | Description                              |
|-------------------------------|------------|------------------------------------------|
| order_id                      | VARCHAR(50) PK | Unique order ID                      |
| customer_id                   | VARCHAR(50) FK → olist_customers | Customer who placed order |
| order_status                  | VARCHAR(20)| delivered/shipped/canceled/unavailable/processing/created/approved/invoiced |
| order_purchase_timestamp      | DATETIME   | When order was placed                    |
| order_approved_at             | DATETIME   | When payment was approved                |
| order_delivered_carrier_date  | DATETIME   | When handed to carrier                   |
| order_delivered_customer_date | DATETIME   | When delivered to customer                |
| order_estimated_delivery_date | DATETIME   | Estimated delivery date                  |

### Table: olist_order_items
| Column              | Type          | Description                                |
|---------------------|---------------|--------------------------------------------|
| id                  | INTEGER PK    | Auto-increment ID                          |
| order_id            | VARCHAR(50) FK → olist_orders | Which order                  |
| order_item_id       | INTEGER       | Item sequence number within order (1,2,3..)|
| product_id          | VARCHAR(50) FK → olist_products | Which product              |
| seller_id           | VARCHAR(50) FK → olist_sellers  | Which seller fulfilled it  |
| shipping_limit_date | DATETIME      | Seller shipping deadline                   |
| price               | DECIMAL(10,2) | Item price in BRL (R$)                     |
| freight_value       | DECIMAL(10,2) | Shipping cost in BRL (R$)                  |

### Table: olist_order_payments
| Column               | Type          | Description                              |
|----------------------|---------------|------------------------------------------|
| id                   | INTEGER PK    | Auto-increment ID                        |
| order_id             | VARCHAR(50) FK → olist_orders | Which order                |
| payment_sequential   | INTEGER       | Payment sequence (1 = first payment)     |
| payment_type         | VARCHAR(30)   | credit_card / boleto / voucher / debit_card |
| payment_installments | INTEGER       | Number of installments chosen            |
| payment_value        | DECIMAL(10,2) | Payment amount in BRL (R$)               |

### Table: olist_order_reviews
| Column                  | Type        | Description                              |
|-------------------------|-------------|------------------------------------------|
| id                      | INTEGER PK  | Auto-increment ID                        |
| review_id               | VARCHAR(50) | Unique review ID                         |
| order_id                | VARCHAR(50) FK → olist_orders | Reviewed order           |
| review_score            | INTEGER     | Rating 1-5 stars                         |
| review_comment_title    | TEXT        | Review title (may be NULL, Portuguese)   |
| review_comment_message  | TEXT        | Review text (may be NULL, Portuguese)    |
| review_creation_date    | DATETIME    | When review was written                  |
| review_answer_timestamp | DATETIME    | When seller responded                    |

### Table: olist_geolocation
| Column                       | Type        | Description                        |
|------------------------------|-------------|------------------------------------|
| id                           | INTEGER PK  | Auto-increment ID                  |
| geolocation_zip_code_prefix  | VARCHAR(10) | Zip code prefix                    |
| geolocation_lat              | FLOAT       | Latitude                           |
| geolocation_lng              | FLOAT       | Longitude                          |
| geolocation_city             | VARCHAR(100)| City name                          |
| geolocation_state            | VARCHAR(5)  | State abbreviation                 |

### KEY RELATIONSHIPS:
- olist_orders.customer_id → olist_customers.customer_id
- olist_order_items.order_id → olist_orders.order_id
- olist_order_items.product_id → olist_products.product_id
- olist_order_items.seller_id → olist_sellers.seller_id
- olist_order_payments.order_id → olist_orders.order_id
- olist_order_reviews.order_id → olist_orders.order_id
- olist_products.product_category_name → product_category_translation.product_category_name

### IMPORTANT NOTES:
- Currency is BRL (Brazilian Real, R$)
- Dates range from 2016 to 2018
- Categories are in Portuguese; use product_category_translation table to get English names
- customer_id is per-order; use customer_unique_id to count unique customers
- Most orders have status "delivered"
- Brazilian states: SP=São Paulo, RJ=Rio de Janeiro, MG=Minas Gerais, etc.
"""

# ============================================
# 2. FEW-SHOT EXAMPLES
# ============================================
FEW_SHOT_EXAMPLES = """
### Example Questions and SQL:

Q: "Show me total revenue by month"
SQL:
```sql
SELECT strftime('%Y-%m', o.order_purchase_timestamp) as month,
       COUNT(DISTINCT o.order_id) as total_orders,
       ROUND(SUM(p.payment_value), 2) as revenue
FROM olist_orders o
JOIN olist_order_payments p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY month
ORDER BY month
LIMIT 50;
```

Q: "Top 10 product categories by sales"
SQL:
```sql
SELECT t.product_category_name_english as category,
       COUNT(DISTINCT oi.order_id) as total_orders,
       ROUND(SUM(oi.price), 2) as total_sales,
       ROUND(AVG(oi.price), 2) as avg_price
FROM olist_order_items oi
JOIN olist_products p ON oi.product_id = p.product_id
JOIN product_category_translation t ON p.product_category_name = t.product_category_name
JOIN olist_orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY t.product_category_name_english
ORDER BY total_sales DESC
LIMIT 10;
```

Q: "Which states have the most customers?"
SQL:
```sql
SELECT c.customer_state as state,
       COUNT(DISTINCT c.customer_unique_id) as unique_customers,
       COUNT(DISTINCT o.order_id) as total_orders,
       ROUND(SUM(pay.payment_value), 2) as total_spent
FROM olist_customers c
JOIN olist_orders o ON c.customer_id = o.customer_id
JOIN olist_order_payments pay ON o.order_id = pay.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY unique_customers DESC
LIMIT 15;
```

Q: "Average review score by product category"
SQL:
```sql
SELECT t.product_category_name_english as category,
       COUNT(r.id) as review_count,
       ROUND(AVG(r.review_score), 2) as avg_rating,
       SUM(CASE WHEN r.review_score >= 4 THEN 1 ELSE 0 END) as positive_reviews,
       SUM(CASE WHEN r.review_score <= 2 THEN 1 ELSE 0 END) as negative_reviews
FROM olist_order_reviews r
JOIN olist_orders o ON r.order_id = o.order_id
JOIN olist_order_items oi ON o.order_id = oi.order_id
JOIN olist_products p ON oi.product_id = p.product_id
JOIN product_category_translation t ON p.product_category_name = t.product_category_name
GROUP BY t.product_category_name_english
HAVING review_count >= 10
ORDER BY avg_rating DESC
LIMIT 15;
```

Q: "Payment method distribution"
SQL:
```sql
SELECT payment_type,
       COUNT(*) as usage_count,
       ROUND(AVG(payment_value), 2) as avg_value,
       ROUND(SUM(payment_value), 2) as total_value,
       ROUND(AVG(payment_installments), 1) as avg_installments
FROM olist_order_payments
GROUP BY payment_type
ORDER BY total_value DESC;
```
"""

# ============================================
# 3. SYSTEM PROMPT
# ============================================
SYSTEM_PROMPT = f"""You are an expert SQL analyst for the Olist Brazilian E-Commerce dataset. Convert natural language questions into accurate SQLite SQL queries.

## Database Schema:
{DATABASE_SCHEMA}

## Few-Shot Examples:
{FEW_SHOT_EXAMPLES}

## Rules:
1. ONLY generate SELECT queries. Never INSERT, UPDATE, DELETE, DROP, ALTER, CREATE.
2. Use proper JOINs based on foreign key relationships shown above.
3. Always use table aliases (o for orders, c for customers, oi for order_items, p for products, etc.).
4. For date functions, use SQLite syntax: strftime('%Y-%m', column), date(), etc.
5. Always add LIMIT (max 500) unless user asks for all data.
6. Use ROUND() for decimal values.
7. For "revenue" or "sales", use SUM(payment_value) from olist_order_payments OR SUM(price) from olist_order_items.
8. For delivered orders, filter: o.order_status = 'delivered'.
9. For English category names, always JOIN with product_category_translation table.
10. For unique customer count, use COUNT(DISTINCT customer_unique_id).
11. Boolean/NULL handling: use IS NULL / IS NOT NULL for missing data.
12. Currency is BRL (Brazilian Real).

## Response Format:
Respond with ONLY a JSON object (no markdown, no extra text):
{{"sql": "YOUR SQL QUERY HERE", "explanation": "Brief plain English explanation"}}
"""

CORRECTION_PROMPT = """The SQL query produced an error. Fix it.

Original question: {question}
Generated SQL: {sql}
Error: {error}

Remember: SQLite syntax, check table/column names against schema, correct JOINs.
Return ONLY: {{"sql": "FIXED SQL", "explanation": "What this query does"}}
"""


# ============================================
# 4. SQL VALIDATOR
# ============================================
class SQLValidator:
    BLOCKED_KEYWORDS = [
        'INSERT', 'UPDATE', 'DELETE', 'DROP', 'ALTER', 'CREATE',
        'TRUNCATE', 'REPLACE', 'MERGE', 'EXEC', 'EXECUTE',
        'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK', 'SAVEPOINT',
        'ATTACH', 'DETACH', 'PRAGMA',
    ]

    @classmethod
    def validate(cls, sql: str) -> dict:
        if not sql or not sql.strip():
            return {"valid": False, "error": "Empty SQL query"}

        sql_upper = sql.upper().strip()

        if not sql_upper.startswith('SELECT') and not sql_upper.startswith('WITH'):
            return {"valid": False, "error": "Only SELECT queries are allowed"}

        for keyword in cls.BLOCKED_KEYWORDS:
            if re.search(r'\b' + keyword + r'\b', sql_upper):
                return {"valid": False, "error": f"Forbidden keyword: {keyword}"}

        statements = [s.strip() for s in sql.split(';') if s.strip()]
        if len(statements) > 1:
            return {"valid": False, "error": "Multiple statements not allowed"}

        return {"valid": True, "error": None}


# ============================================
# 5. SQL EXECUTOR
# ============================================
class SQLExecutor:
    @staticmethod
    def execute(sql: str, max_rows: int = None) -> dict:
        if max_rows is None:
            max_rows = getattr(settings, 'NL2SQL_MAX_ROWS', 500)

        if 'LIMIT' not in sql.upper():
            sql = sql.rstrip(';') + f' LIMIT {max_rows}'

        db_path = settings.DATABASES['default']['NAME']
        start_time = time.time()

        try:
            conn = sqlite3.connect(str(db_path))
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(sql)

            columns = [desc[0] for desc in cursor.description] if cursor.description else []
            rows_raw = cursor.fetchall()

            rows = []
            for row in rows_raw:
                row_dict = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    if isinstance(val, Decimal):
                        val = float(val)
                    elif isinstance(val, (datetime, date)):
                        val = val.isoformat()
                    row_dict[col] = val
                rows.append(row_dict)

            execution_time = (time.time() - start_time) * 1000
            conn.close()

            return {
                "success": True,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "execution_time_ms": round(execution_time, 2),
            }
        except Exception as e:
            return {
                "success": False,
                "error": str(e),
                "columns": [], "rows": [],
                "row_count": 0, "execution_time_ms": 0,
            }


# ============================================
# 6. CLAUDE API CLIENT
# ============================================
class ClaudeClient:
    @staticmethod
    def generate_sql(question: str, error_context: str = None) -> dict:
        import anthropic


        api_key = settings.OPENAI_API_KEY
        if not api_key:
            return {"error": "OPENAI_API_KEY not configured. Set it in environment variables."}

        try:
            client = anthropic.Anthropic(api_key=api_key)

            user_message = error_context if error_context else f"Convert this question to SQL: {question}"

            response = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1024,
                system=SYSTEM_PROMPT,
                messages=[{"role": "user", "content": user_message}]
            )

            response_text = response.content[0].text.strip()

            if response_text.startswith('```'):
                response_text = re.sub(r'^```(?:json)?\s*', '', response_text)
                response_text = re.sub(r'\s*```$', '', response_text)

            result = json.loads(response_text)
            if 'sql' not in result:
                return {"error": "Claude did not return a valid SQL query"}

            return {"sql": result['sql'].strip(), "explanation": result.get('explanation', '')}

        except json.JSONDecodeError:
            sql_match = re.search(r'(?:SELECT|WITH)\s+.+', response_text, re.IGNORECASE | re.DOTALL)
            if sql_match:
                return {"sql": sql_match.group(0).strip().rstrip(';'), "explanation": ""}
            return {"error": "Failed to parse Claude's response"}
        except Exception as e:
            return {"error": f"Claude API error: {str(e)}"}


# ============================================
# 7. MAIN NL2SQL ENGINE
# ============================================
class NL2SQLEngine:
    def __init__(self):
        self.validator = SQLValidator()
        self.executor = SQLExecutor()
        self.claude = ClaudeClient()
        self.max_retries = getattr(settings, 'NL2SQL_MAX_RETRIES', 2)

    def process_question(self, question: str) -> dict:
        result = {
            "success": False, "question": question,
            "sql": "", "explanation": "",
            "columns": [], "rows": [],
            "row_count": 0, "execution_time_ms": 0,
            "attempts": 0, "error": None,
        }

        claude_response = self.claude.generate_sql(question)
        if 'error' in claude_response:
            result["error"] = claude_response["error"]
            return result

        sql = claude_response["sql"]
        explanation = claude_response.get("explanation", "")
        result["sql"] = sql
        result["explanation"] = explanation
        result["attempts"] = 1

        validation = self.validator.validate(sql)
        if not validation["valid"]:
            result["error"] = f"Validation failed: {validation['error']}"
            return result

        for attempt in range(self.max_retries + 1):
            exec_result = self.executor.execute(sql)

            if exec_result["success"]:
                result.update({
                    "success": True, "sql": sql,
                    "columns": exec_result["columns"],
                    "rows": exec_result["rows"],
                    "row_count": exec_result["row_count"],
                    "execution_time_ms": exec_result["execution_time_ms"],
                    "attempts": attempt + 1,
                })
                return result

            if attempt < self.max_retries:
                fix_response = self.claude.generate_sql(
                    question,
                    error_context=CORRECTION_PROMPT.format(
                        question=question, sql=sql, error=exec_result["error"]
                    )
                )
                if 'error' in fix_response:
                    result["error"] = fix_response["error"]
                    return result

                sql = fix_response["sql"]
                result["sql"] = sql
                result["explanation"] = fix_response.get("explanation", explanation)
                result["attempts"] = attempt + 2

                validation = self.validator.validate(sql)
                if not validation["valid"]:
                    result["error"] = f"Corrected SQL validation failed: {validation['error']}"
                    return result
            else:
                result["error"] = f"Failed after {self.max_retries + 1} attempts: {exec_result['error']}"

        return result


# ============================================
# 8. DEMO MODE (No API Key)
# ============================================
class DemoNL2SQLEngine:
    DEMO_QUERIES = {
        'revenue|sales|monthly': {
            'sql': """SELECT strftime('%Y-%m', o.order_purchase_timestamp) as month,
       COUNT(DISTINCT o.order_id) as total_orders,
       ROUND(SUM(p.payment_value), 2) as revenue,
       ROUND(AVG(p.payment_value), 2) as avg_order_value
FROM olist_orders o
JOIN olist_order_payments p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY month
ORDER BY month
LIMIT 24""",
            'explanation': 'Monthly revenue trend for delivered orders (in BRL).'
        },
        'category|categories|product type': {
            'sql': """SELECT t.product_category_name_english as category,
       COUNT(DISTINCT oi.order_id) as total_orders,
       ROUND(SUM(oi.price), 2) as total_sales,
       ROUND(AVG(oi.price), 2) as avg_price
FROM olist_order_items oi
JOIN olist_products p ON oi.product_id = p.product_id
JOIN product_category_translation t ON p.product_category_name = t.product_category_name
JOIN olist_orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY t.product_category_name_english
ORDER BY total_sales DESC
LIMIT 15""",
            'explanation': 'Top 15 product categories by total sales value.'
        },
        'state|states|customer location|region': {
            'sql': """SELECT c.customer_state as state,
       COUNT(DISTINCT c.customer_unique_id) as unique_customers,
       COUNT(DISTINCT o.order_id) as total_orders,
       ROUND(SUM(pay.payment_value), 2) as total_revenue
FROM olist_customers c
JOIN olist_orders o ON c.customer_id = o.customer_id
JOIN olist_order_payments pay ON o.order_id = pay.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_state
ORDER BY total_revenue DESC
LIMIT 15""",
            'explanation': 'Customer distribution and revenue by Brazilian state.'
        },
        'payment|payment method|credit card|boleto': {
            'sql': """SELECT payment_type,
       COUNT(*) as usage_count,
       ROUND(AVG(payment_value), 2) as avg_value,
       ROUND(SUM(payment_value), 2) as total_value,
       ROUND(AVG(payment_installments), 1) as avg_installments,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM olist_order_payments), 1) as pct
FROM olist_order_payments
GROUP BY payment_type
ORDER BY total_value DESC""",
            'explanation': 'Payment method distribution: credit card, boleto, voucher, debit card.'
        },
        'review|rating|score|satisfaction': {
            'sql': """SELECT review_score,
       COUNT(*) as count,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM olist_order_reviews), 1) as percentage
FROM olist_order_reviews
GROUP BY review_score
ORDER BY review_score DESC""",
            'explanation': 'Distribution of review scores (1-5 stars).'
        },
        'seller|sellers|top seller': {
            'sql': """SELECT s.seller_city, s.seller_state,
       COUNT(DISTINCT oi.order_id) as orders_fulfilled,
       ROUND(SUM(oi.price), 2) as total_sales,
       ROUND(AVG(oi.price), 2) as avg_item_price,
       COUNT(DISTINCT oi.product_id) as products_sold
FROM olist_sellers s
JOIN olist_order_items oi ON s.seller_id = oi.seller_id
JOIN olist_orders o ON oi.order_id = o.order_id
WHERE o.order_status = 'delivered'
GROUP BY s.seller_city, s.seller_state
ORDER BY total_sales DESC
LIMIT 15""",
            'explanation': 'Top 15 seller cities by total sales.'
        },
        'delivery|shipping|freight|delivery time': {
            'sql': """SELECT c.customer_state as state,
       COUNT(*) as delivered_orders,
       ROUND(AVG(julianday(o.order_delivered_customer_date) - julianday(o.order_purchase_timestamp)), 1) as avg_delivery_days,
       ROUND(AVG(julianday(o.order_estimated_delivery_date) - julianday(o.order_delivered_customer_date)), 1) as avg_days_early,
       ROUND(AVG(oi.freight_value), 2) as avg_freight
FROM olist_orders o
JOIN olist_customers c ON o.customer_id = c.customer_id
JOIN olist_order_items oi ON o.order_id = oi.order_id
WHERE o.order_status = 'delivered'
  AND o.order_delivered_customer_date IS NOT NULL
GROUP BY c.customer_state
ORDER BY avg_delivery_days ASC
LIMIT 15""",
            'explanation': 'Average delivery time and freight cost by state.'
        },
        'order status|status|cancelled|canceled': {
            'sql': """SELECT order_status,
       COUNT(*) as order_count,
       ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM olist_orders), 1) as percentage
FROM olist_orders
GROUP BY order_status
ORDER BY order_count DESC""",
            'explanation': 'Order status distribution across all orders.'
        },
        'customer|top customer|best customer': {
            'sql': """SELECT c.customer_unique_id,
       c.customer_city, c.customer_state,
       COUNT(DISTINCT o.order_id) as total_orders,
       ROUND(SUM(pay.payment_value), 2) as total_spent
FROM olist_customers c
JOIN olist_orders o ON c.customer_id = o.customer_id
JOIN olist_order_payments pay ON o.order_id = pay.order_id
WHERE o.order_status = 'delivered'
GROUP BY c.customer_unique_id, c.customer_city, c.customer_state
ORDER BY total_spent DESC
LIMIT 15""",
            'explanation': 'Top 15 customers by total spending.'
        },
        'heavy|weight|big product|large': {
            'sql': """SELECT t.product_category_name_english as category,
       ROUND(AVG(p.product_weight_g), 0) as avg_weight_g,
       ROUND(AVG(p.product_length_cm * p.product_height_cm * p.product_width_cm), 0) as avg_volume_cm3,
       COUNT(*) as product_count,
       ROUND(AVG(oi.freight_value), 2) as avg_freight
FROM olist_products p
JOIN product_category_translation t ON p.product_category_name = t.product_category_name
JOIN olist_order_items oi ON p.product_id = oi.product_id
WHERE p.product_weight_g IS NOT NULL
GROUP BY t.product_category_name_english
HAVING product_count >= 10
ORDER BY avg_weight_g DESC
LIMIT 15""",
            'explanation': 'Heaviest product categories by average weight and shipping cost.'
        },
    }

    def process_question(self, question: str) -> dict:
        question_lower = question.lower()

        matched_query = None
        best_score = 0

        for keywords_str, query_data in self.DEMO_QUERIES.items():
            keywords = keywords_str.split('|')
            score = sum(1 for kw in keywords if kw in question_lower)
            if score > best_score:
                best_score = score
                matched_query = query_data

        if not matched_query or best_score == 0:
            # Default: revenue overview
            matched_query = list(self.DEMO_QUERIES.values())[0]

        executor = SQLExecutor()
        exec_result = executor.execute(matched_query['sql'])

        return {
            "success": exec_result["success"],
            "question": question,
            "sql": matched_query['sql'],
            "explanation": matched_query['explanation'],
            "columns": exec_result.get("columns", []),
            "rows": exec_result.get("rows", []),
            "row_count": exec_result.get("row_count", 0),
            "execution_time_ms": exec_result.get("execution_time_ms", 0),
            "attempts": 1,
            "error": exec_result.get("error"),
            "demo_mode": True,
        }

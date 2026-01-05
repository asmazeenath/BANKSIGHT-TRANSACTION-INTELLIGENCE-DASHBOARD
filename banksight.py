import pymysql
import mysql.connector as conn
import pandas as pd

connection=pymysql.connect
("""
    host="localhost"
    user="root"
    password="zeenathasma@733"
    database="banksight"
    cursorclass=pymysql.cursor.DictCursor
""")

 try
    with connection.cursor() as cursor:
      CREATE_TABLE=CREATE_TABLE_IF_NOT_EXISTS_customers ("""
  customer_id VARCHAR(50) PRIMARY KEY,
  name VARCHAR(150) NOT NULL,
  gender VARCHAR(10),
  age INT,
  city VARCHAR(100),
  account_type VARCHAR(50),
  join_date DATE
);CREATE_TABLE account_balances (
  customer_id VARCHAR(50) PRIMARY KEY,
  account_balance DECIMAL(15,2),
  last_updated DATE,
  CONSTRAINT fk_acc_customer
    FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id)
);CREATE_TABLE transactions (
  txn_id VARCHAR(50) PRIMARY KEY,
  customer_id VARCHAR(50),
  txn_type VARCHAR(50),
  amount DECIMAL(15,2),
  txn_time DATETIME,
  status VARCHAR(30),
  CONSTRAINT fk_txn_customer
    FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id)
);CREATE_TABLE loans (
  loan_id INT PRIMARY KEY,
  customer_id VARCHAR(50),
  account_id VARCHAR(50),
  branch VARCHAR(100),
  loan_type VARCHAR(50),
  loan_amount INT,
  interest_rate DECIMAL(5,2),
  loan_term_months INT,
  start_date DATE,
  end_date DATE,
  loan_status VARCHAR(30),
  CONSTRAINT fk_loan_customer
    FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id)
);CREATE_TABLE credit_cards (
  card_id INT PRIMARY KEY,
  customer_id VARCHAR(50),
  account_id VARCHAR(50),
  branch VARCHAR(100),
  card_number VARCHAR(30),
  card_type VARCHAR(50),
  card_network VARCHAR(30),
  credit_limit INT,
  current_balance DECIMAL(15,2),
  issued_date DATE,
  expiry_date DATE,
  status VARCHAR(30),
  CONSTRAINT fk_card_customer
    FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id)
);CREATE_TABLE branches (
  branch_id INT PRIMARY KEY,
  branch_name VARCHAR(150),
  city VARCHAR(100),
  manager_name VARCHAR(100),
  total_employees INT,
  branch_revenue DECIMAL(15,2),
  opening_date DATE,
  performance_rating INT
);CREATE_TABLE support_tickets (
  ticket_id VARCHAR(50) PRIMARY KEY,
  customer_id VARCHAR(50),
  account_id VARCHAR(50),
  loan_id VARCHAR(50),
  branch_name VARCHAR(150),
  issue_category VARCHAR(100),
  description TEXT,
  date_opened DATE,
  date_closed DATE,
  priority VARCHAR(20),
  status VARCHAR(30),
  resolution_remarks TEXT,
  support_agent VARCHAR(100),
  channel VARCHAR(30),
  customer_rating INT,
  CONSTRAINT fk_ticket_customer
    FOREIGN KEY (customer_id)
    REFERENCES customers(customer_id)
""");

cursor=conn.cursor()
df = pd.read_csv("csv/account_balance.csv")
df['last_updated'] = pd.to_datetime(df['last_updated'])

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO account_balance
        (customer_id, account_balance, last_updated)
        VALUES (%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        account_balance=VALUES(account_balance)
    """, tuple(row))
    df = pd.read_csv("csv/branches.csv")
df['opening_date'] = pd.to_datetime(df['opening_date'])

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO branches
        (branch_id, branch_name, city, manager,
         total_employees, branch_revenue,
         opening_date, performance_rating)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        performance_rating=VALUES(performance_rating)
    """, tuple(row))
    
    df = pd.read_csv("csv/credit_cards.csv")
df['issued_date'] = pd.to_datetime(df['issued_date'])
df['expiry_date'] = pd.to_datetime(df['expiry_date'])

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO credit_cards
        (card_id, customer_id, account_id, branch,
         card_number, card_type, card_network,
         credit_limit, current_balance,
         issued_date, expiry_date, status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        status=VALUES(status)
    """, tuple(row))

    df = pd.read_csv("csv/customers.csv")

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO customers
        (customer_id, name, gender, age, city, account_type, join_date)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        name=VALUES(name)
    """, tuple(row))

    df = pd.read_csv("csv/loans.csv")
df['start_date'] = pd.to_datetime(df['start_date'])
df['end_date'] = pd.to_datetime(df['end_date'])

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO loans
        (loan_id, customer_id, account_id, branch, loan_type,
         loan_amount, interest_rate, loan_term_months,
         start_date, end_date, loan_status)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE loan_status=VALUES(loan_status)
    """, tuple(row))

    df = pd.read_csv("csv/support_tickets.csv")
df['date_opened'] = pd.to_datetime(df['date_opened'])
df['date_closed'] = pd.to_datetime(df['date_closed'], errors='coerce')

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO support_tickets
        (ticket_id, customer_id, account_id, loan_id, branch_name,
         issue_category, issue_description, date_opened,
         date_closed, priority, status, resolution_remarks)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE status=VALUES(status)
    """, tuple(row))

    df = pd.read_csv("csv/transactions.csv")
df['txn_time'] = pd.to_datetime(df['txn_time'], errors='coerce')

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO transactions
        (txn_id, customer_id, txn_type, amount, txn_time, status)
        VALUES (%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE amount=VALUES(amount)
    """, tuple(row))


def run_query(title, query):
    print(f"\n--- {title} ---")
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        print(row)

#CUSTOMERS AND ACCOUNT ANALYSIS 

run_query("""
    "Q1:How Many Customers Exist Per City,And What is their Average Account Balance?"
    SELECT customers.CITY,
COUNT(customers.customer_id)AS
TOTAL_CUSTOMERS,
AVG(account_balances.account_balance)AS
AVERAGE_BALANCES
FROM customers
LEFT JOIN account_balances
ON customers.customer_id=account_balances.customer_id
group by customers.city;"""

) 
run_query("""
    "Q2:Which Account Type Holds the Highest Total Balance?"
    SELECT customers.account_type,
SUM(account_balances.account_balance)AS total_balance
FROM customers
JOIN account_balances
ON customers.customer_id=customers.customer_id
GROUP BY customers.account_type
ORDER by total_balance DESC
LIMIT 1;"""

)
run_query("""
    "Q3:Who are the Top 10 Customers By Total Account Balance Across All Account Types?"
    select customers.customer_id,
customers.name,
SUM(account_balances.account_balance)
AS total_balance
FROM customers
JOIN account_balances
ON customers.customer_id=account_balances.customer_id
group by customers.customer_id,customers.name
ORDER BY total_balance DESC
LIMIT 10;"""
)
run_query ("""
    "Q4:Which Customers Opened Accounts in 2023 with a Balance Above ₹1,00,000?"
    SELECT 
    customers.customer_id,
    customers.name,
    account_balances.account_balance,
    customers.join_date
FROM customers
JOIN account_balances
ON customers.customer_id = account_balances.customer_id
WHERE YEAR(customers.join_date) = 2023
  AND account_balances.account_balance > 100000;"""
)
run_query
(""""Q5:What is the Total Transaction Volume by Transaction Type?"
 SELECT
    transactions.txn_type,
    SUM(amount) AS total_transaction_volume
FROM transactions
GROUP BY txn_type;
          )
run_query("Q6:How Many Failed Transaction Occured For Each Transaction Type?"
  SELECT transactions.txn_type,
count(*) as FAILED_TRANSACTIONS
FROM transactions
WHERE status='failed'
group by txn_type;"""
         )
run_query
(""""Q7:What is the Total Number of Transactions Per Transaction Type?"
 SELECT transactions.txn_type,
sum(amount)as TOTAL_NUMBER_OF_TRANSACTION
FROM transactions
group by txn_type;""")
run_query
(""""Q8:Which Accounts Have 5 or More High-Value Transactions Above ₹20,000?"
 SELECT 
    cc.account_id,
    COUNT(*) AS high_value_txn_count
FROM transactions t
JOIN credit_cards cc
    ON t.amount = cc.account_id
WHERE CAST(REPLACE(t.amount, ',', '') AS DECIMAL(10,2)) > 20000
GROUP BY cc.account_id
HAVING COUNT(*) >= 5;""")
#LOANS
run_query
(""""Q9:What is the average loan amount and interest rate by loan type ?"
 SELECT 
    loan_type,
    AVG(CAST(REPLACE(loan_amount, ',', '') AS DECIMAL(12,2))) AS avg_loan_amount,
    AVG(CAST(REPLACE(interest_rate, '%', '') AS DECIMAL(5,2))) AS avg_interest_rate
FROM loans
GROUP BY loan_type;""")
run_query(""""Q10: Which customers currently hold more than one active or approved loan?"
          SELECT 
    loans.customer_id,
    customers.name,
    
    COUNT(*) AS active_approved_loan_count
FROM loans
join customers
on loans.customer_id=loans.customer_id
WHERE loan_status IN ('Active', 'Approved')
GROUP BY customer_id,name
HAVING COUNT(*) > 1;""")
run_query
(""""Q11:Who are the top 5 customers with the highest outstanding (non-closed) loan amount"
 SELECT 
    c.customer_id,
    c.name,
    SUM(l.loan_amount) AS total_loan
FROM loans l
JOIN customers c
    ON l.customer_id = l.customer_id
WHERE l.loan_status != 'ACTIVE''APPROVED''DEFAULTED'
GROUP BY c.customer_id, c.name
ORDER BY total_loan DESC
LIMIT 5;"""
)
# BRANCH & PERFORMANCE
run_query
(""""Q12: What is the average loan amount per branch?"
 SELECT 
    b.branch_name,
    AVG(l.loan_amount) AS avg_loan_amount
FROM loans l
JOIN branches b
    ON l.branch = l.branch
GROUP BY b.branch_name;"""
)
run_query
(""""Q13:How many customers exist in each age group?"
 SELECT 
    CASE
        WHEN age BETWEEN 18 AND 25 THEN '18–25'
        WHEN age BETWEEN 26 AND 35 THEN '26–35'
        WHEN age BETWEEN 36 AND 45 THEN '36–45'
        WHEN age BETWEEN 46 AND 60 THEN '46–60'
        ELSE '60+'
    END AS age_group,
    COUNT(*) AS customer_count
FROM customers
GROUP BY age_group
ORDER BY age_group;""")
#SUPPORT TICKETS & CUSTOMER EXPERIENCE
run_query
("Q14:Which issue categories have the longest average resolution time?"
   """SELECT
    issue_category,
    AVG(DATEDIFF(date_closed,date_opened)) AS avg_resolution_days
FROM
    support_tickets
WHERE
    date_closed IS NOT NULL
GROUP BY
    issue_category
ORDER BY
    avg_resolution_days DESC;""")
run_query
("Q15: Which support agents have resolved the most critical tickets with high customer ratings (≥4)?"

"""SELECT
    support_agent,
    COUNT(*) AS critical_high_rated_tickets
FROM
    support_tickets
WHERE
    priority = 'Critical'
    AND customer_rating >= 4
    AND status = 'Resolved'
GROUP BY
    support_agent
ORDER BY
    critical_high_rated_tickets DESC
LIMIT 1;""")
cursor.close()
conn.close()
print("\n program completed successfully")

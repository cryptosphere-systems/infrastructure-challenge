import os
import boto3
import requests
import psycopg2


def get_secret_from_ssm(key):
    ssm = boto3.client("ssm", region_name="eu-west-2")
    resp = ssm.get_parameter(Name=key, WithDecryption=True)
    return resp["Parameter"]["Value"]


def execute_sql(sql):
    conn = None
    try:
        conn = psycopg2.connect(
            host=os.environ.get("DB_HOST"),
            database=os.environ.get("DB_DATABASE"),
            user=os.environ.get("DB_USER"),
            password=get_secret_from_ssm(os.environ.get("DB_PASSWORD"))
        )
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        raise
    finally:
        if conn is not None:
            conn.close()


def create_table():
    """Creates the db table"""
    sql = """CREATE TABLE IF NOT EXISTS prices (
                    id SERIAL PRIMARY KEY,
                    created_at TIMESTAMPTZ NOT NULL DEFAULT (NOW()),
                    price VARCHAR(50) NOT NULL
                );"""
    print("creating table...")
    execute_sql(sql)
    print("Table was created.")


def insert_price(price: str):
    """ insert a new price into the prices table """
    sql = f"INSERT INTO prices(price) VALUES('{price}');"
    print("Inserting price")
    execute_sql(sql)
    print("insert completed.")


def lambda_handler(event, context):
    r = requests.get("https://api.coingecko.com/api/v3/simple/price?ids=monero&vs_currencies=eur")
    result = r.json()
    # price will be a float like 201.34
    price_xmr_euro = str(result["monero"]["eur"])
    # Generally, using 'logging' module is best but
    # stdout goes to Lambda logs, so 'print' works for logging also
    print(f"price_xmr_euro:{price_xmr_euro}")
    try:
        insert_price(price_xmr_euro)
    except Exception as e:
        create_table()

    return {
        'statusCode': 200,
        'body': r.json()
    }

#!/bin/bash

# var by default

echo "=== Init script ==="

until pg_isready -U "$POSTGRES_USER" -d postgres ; do
    echo "Waiting for PostgreSQL to start..."
    sleep 2
done

echo "PostgreSQL is ready, initializing databases..."

echo "=== Initializing databases ==="

exec_sql() {
    PGPASSWORD=$POSTGRES_PASSWORD psql  -U "$POSTGRES_USER" -d postgres -c "$1" 2>/dev/null || true 
}


DB_EXISTS=$(PGPASSWORD=$POSTGRES_PASSWORD psql  -U "$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$METABASE_DB'")

if [ "$DB_EXISTS" != "1" ]; then
    echo "Creating database: $METABASE_DB"
    exec_sql "CREATE DATABASE $METABASE_DB"
    echo "Database created"
else
    echo "Database already exists"
fi

exec_sql "GRANT ALL PRIVILEGES ON DATABASE $METABASE_DB TO $POSTGRES_USER"

echo "Database $METABASE_DB initialized successfully"

# Create abb db
APP_DB_EXISTS=$(PGPASSWORD=$POSTGRES_PASSWORD psql  -U "$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='$APP_DB'")

if [ "$APP_DB_EXISTS" != "1" ]; then
    echo "Creating database: $APP_DB"
    exec_sql "CREATE DATABASE $APP_DB"
    echo "Database created $APP_DB"
else
    echo "Database $APP_DB already exists"
fi


exec_sql "GRANT ALL PRIVILEGES ON DATABASE $APP_DB TO $POSTGRES_USER"
exec_sql "CREATE SCHEMA IF NOT EXISTS $DB_SHEMA AUTHORIZATION $POSTGRES_USER"
echo "$APP_DB ready"

# user add
if [ -n "$APP_USER" ] && [ "$APP_USER" != "$POSTGRES_USER" ]; then
    USER_EXISTS=$(PGPASSWORD=$POSTGRES_PASSWORD psql  -U "$POSTGRES_USER" -d postgres -tAc "SELECT 1 FROM pg_roles WHERE rolname='$APP_USER'")

    if [ "$USER_EXISTS" != "1" ]; then
        echo "Creating user: $APP_USER"
        exec_sql "CREATE USER $APP_USER WITH PASSWORD '$APP_USER_PASSWORD'"
        exec_sql "GRANT CONNECT ON DATABASE $APP_DB TO $APP_USER"
        echo "User created"
    else
        echo "User already exists"
    fi
fi

echo "User ready"


PGPASSWORD=$POSTGRES_PASSWORD psql  -U "$POSTGRES_USER" -d "$APP_DB" <<EOF
CREATE SCHEMA IF NOT EXISTS $DB_SCHEMA AUTHORIZATION $APP_USER;

CREATE TABLE IF NOT EXISTS $DB_SCHEMA.logs (
    id SERIAL PRIMARY KEY,
    date TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    level TEXT, 
    message TEXT
);

CREATE TABLE IF NOT EXISTS $DB_SCHEMA.sales_details(
    id SERIAL PRIMARY KEY,
    client_id INT,
    gender CHAR(1),
    purchase_datetime DATE,
    purchase_time_as_seconds_from_midnight INT,
    product_id INT,
    quantity INT,
    price_per_item NUMERIC(12,2),
    discount_per_item NUMERIC(12,2),
    total_price NUMERIC(12,2)
);

CREATE INDEX idx_sales_details_client ON sales_details(client_id);
CREATE INDEX idx_sales_details_datetime ON sales_details(purchase_datetime);
CREATE INDEX idx_sales_details_product ON sales_details(product_id);
CREATE INDEX idx_sales_details_gender ON sales_details(gender);


CREATE OR REPLACE VIEW $DB_SCHEMA.v_calendar AS
select 
    d::DATE as calendar_date,
    EXTRACT(YEAR FROM d)::INT AS year,
    EXTRACT(QUARTER FROM d)::INT AS quarter,
    EXTRACT(MONTH FROM d)::INT AS month,
    EXTRACT(DAY FROM d)::INT AS day,
    EXTRACT(WEEK FROM d)::INT AS week_of_year,
    EXTRACT(ISODOW FROM d)::INT AS day_of_week, -- 1 (Понедельник) - 7 (Воскресенье)
    TO_CHAR(d, 'TMDay') AS day_name,
    TO_CHAR(d, 'TMMonth') AS month_name,
    CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM generate_series((select min(purchase_datetime)::DATE from $DB_SCHEMA.sales_details), (select max(purchase_datetime)::DATE from $DB_SCHEMA.sales_details), '1 day'::INTERVAL) d;


GRANT ALL PRIVILEGES ON SCHEMA $DB_SCHEMA TO $APP_USER;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA $DB_SCHEMA TO $APP_USER;
GRANT USAGE, SELECT,  UPDATE ON ALL SEQUENCES IN SCHEMA $DB_SCHEMA TO $APP_USER;

CREATE USER user1 WITH PASSWORD 'user1';
GRANT CONNECT ON DATABASE $APP_DB TO user1;
GRANT USAGE ON SCHEMA $DB_SCHEMA TO user1;
GRANT SELECT ON ALL TABLES IN SCHEMA $DB_SCHEMA TO user1;
GRANT SELECT, USAGE ON ALL SEQUENCES IN SCHEMA $DB_SCHEMA TO user1;
ALTER DEFAULT PRIVILEGES IN SCHEMA $DB_SCHEMA GRANT SELECT ON TABLES TO user1;

EOF

echo "=== Database initialization completed ==="

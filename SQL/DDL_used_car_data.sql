-- DAILY_SINK TABLE
CREATE TABLE daily_sink (
    listing_id VARCHAR(20),
    car_model VARCHAR(80),
    title NVARCHAR(MAX),
    price VARCHAR(80),
    variant VARCHAR(20),
    transmission VARCHAR(20),
    installment VARCHAR(80),
    year INT,
    mileage VARCHAR(80),
    color VARCHAR(20),
    location VARCHAR(80),
    state VARCHAR(80),
    url NVARCHAR(MAX),
    image NVARCHAR(MAX),
    extracted_date DATETIME
);

-- STAGING_TABLE
CREATE TABLE staging_table (
    listing_id VARCHAR(20),
    car_model VARCHAR(80),
    title NVARCHAR(MAX),
    price VARCHAR(80),
    variant VARCHAR(20),
    transmission VARCHAR(20),
    installment VARCHAR(80),
    year INT,
    mileage VARCHAR(80),
    color VARCHAR(20),
    location VARCHAR(80),
    state VARCHAR(80),
    url NVARCHAR(MAX),
    image NVARCHAR(MAX),
    extracted_date DATETIME,
    processing_date DATETIME
);

-- PROD_TABLE (Regular Table)
CREATE TABLE prod_table (
    listing_id VARCHAR(20) PRIMARY KEY,
    dim_car_model VARCHAR(20),
    title VARCHAR(80),
    fct_price NUMERIC(18,0),
    dim_variant VARCHAR(20),
    transmission VARCHAR(20),
    fct_installment NUMERIC(18,0),
    fct_year INT,
    fct_mileage NUMERIC(18,0),
    dim_color VARCHAR(20),
    dim_location VARCHAR(80),
    dim_state VARCHAR(20),
    url NVARCHAR(MAX),
    dim_extracted_date DATETIME,
    dim_processing_date DATETIME
);

-- RECENT_LISTINGS TABLE (Tracking Processed Listings)
CREATE TABLE used_car_data.recent_listings (
    listing_id VARCHAR(20) PRIMARY KEY,
    car_model VARCHAR(80),
    price NUMERIC(18,0),
    extracted_date DATE
);

-- TRADEMARK_TABLE (ETL Tracking)
CREATE TABLE used_car_data.trademark_table (
    batch_id VARCHAR(20),
    job_name VARCHAR(50),
    processed_time DATETIME,
    status VARCHAR(20) CHECK (status IN ('Success', 'Failed', 'Pending')),
    row_count NUMERIC(18,0),
    PRIMARY KEY (batch_id, job_name)
);

-- LISTING_IMAGES TABLE (Normalization for Images)
-- LISTING_IMAGES TABLE with FOREIGN KEY
CREATE TABLE used_car_data.listing_images (
    listing_id VARCHAR(20),
    image_url NVARCHAR(255),
    PRIMARY KEY (listing_id, image_url),
    FOREIGN KEY (listing_id) REFERENCES used_car_data.prod_table(listing_id) ON DELETE CASCADE
);

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
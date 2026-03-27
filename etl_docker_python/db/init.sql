CREATE TABLE IF NOT EXISTS breweries (
    brewery_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255),
    brewery_type VARCHAR(100),
    street VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    postal_code VARCHAR(30),
    country VARCHAR(100),
    longitude DOUBLE PRECISION,
    latitude DOUBLE PRECISION,
    phone VARCHAR(50),
    website_url TEXT,
    state_province VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

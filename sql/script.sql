-- Buat database
CREATE DATABASE de_project_pacmann;

-- Pindah ke database yang baru dibuat
\c de_project_pacmann

-- Buat ENUM types
CREATE TYPE condition_type AS ENUM ('New', 'Used', 'Undefined');
CREATE TYPE availability_type AS ENUM ('Unavailable', 'Available', 'Pending');
CREATE TYPE shipping_type AS ENUM ('Free Shipping', 'Expedited Shipping', 'Standard Shipping', 'Freight Shipping', 'Paid Shipping', 'Undefined');

-- Buat tabel sales_clean
CREATE TABLE sales_clean (
    name_product TEXT NOT NULL,
    category VARCHAR(50) NOT NULL,
    link TEXT NOT NULL,
    rating TEXT NOT NULL,
    rating_count TEXT NOT NULL,
    price_original_rupee NUMERIC NOT NULL,
    price_sale_rupee NUMERIC NOT NULL,
    discount TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Buat tabel marketing_clean
CREATE TABLE marketing_clean (
    name_product TEXT NOT NULL,
    category VARCHAR(50) NOT NULL,
    price_original NUMERIC NOT NULL,
    price_sale NUMERIC NOT NULL,
    discount TEXT NOT NULL,
    condition condition_type NOT NULL,
    availability availability_type NOT NULL,
    shipping shipping_type NOT NULL,
    date_seen DATE NOT NULL,
    weight_kg NUMERIC NOT NULL, 
    merchant VARCHAR NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Buat tabel torch_tokped_clean
CREATE TABLE torch_tokped_clean (
    name_product TEXT NOT NULL,
    product_link TEXT NOT NULL,
    price_original INT NOT NULL,
    price_sale INT NOT NULL,
    discount TEXT NOT NULL,
    rating TEXT NOT NULL,
    sold INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

-- Buat tabel torch_lazada_clean
CREATE TABLE torch_lazada_clean (
    name_product TEXT NOT NULL,
    product_link TEXT NOT NULL,
    price_original INT NOT NULL,
    price_sale INT NOT NULL,
    discount TEXT NOT NULL,
    sold INT NOT NULL,
    rating TEXT NOT NULL,
    rating_count INT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

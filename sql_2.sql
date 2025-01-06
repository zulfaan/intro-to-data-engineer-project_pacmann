CREATE TYPE condition_type AS ENUM ('New', 'Used', 'Undefined');
CREATE TYPE availability_type AS ENUM ('Unavailable', 'Available', 'Pending');
CREATE TYPE shipping_type AS ENUM ('Free Shipping', 'Expedited Shipping', 'Standard Shipping', 'Freight Shipping', 'Paid Shipping', 'Undefined');
CREATE TYPE sold_info_type AS ENUM ('+ terjual', 'Belum Ada Penjualan');

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

CREATE TABLE torch_tokped_clean (
    name_product TEXT NOT NULL,
    product_link TEXT NOT NULL,
    price_original INT NOT NULL,
    price_sale INT NOT NULL,
    discount TEXT NOT NULL,
    rating TEXT NOT NULL,
    sold_numeric INT NOT NULL,
    sold_info sold_info_type NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

CREATE TABLE torch_lazada_clean (
    name_product TEXT NOT NULL,
    product_link TEXT NOT NULL,
    price_original INT NOT NULL,
    price_sale INT NOT NULL,
    discount TEXT NOT NULL,
    sold TEXT NOT NULL,
    rating TEXT NOT NULL,
    rating_count TEXT NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT now()
);

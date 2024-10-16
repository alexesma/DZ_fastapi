Importing brand model
Using database URL: postgresql+asyncpg://admin:1qw2%%23ER%%24@dz_db_dev:5432/test_dbname?async_fallback=True
BEGIN;

CREATE TABLE alembic_version (
    version_num VARCHAR(32) NOT NULL, 
    CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num)
);

-- Running upgrade  -> 3e7844c76e31

CREATE TABLE brand (
    name VARCHAR(256) NOT NULL, 
    country_of_origin VARCHAR(100), 
    website VARCHAR(1056), 
    description TEXT, 
    logo VARCHAR(1056), 
    main_brand BOOLEAN, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_name_brand CHECK (name ~ '^[a-zA-Z0-9]+$'), 
    UNIQUE (name)
);

CREATE TABLE category (
    name VARCHAR(256) NOT NULL, 
    parent_id INTEGER, 
    comment TEXT, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(parent_id) REFERENCES category (id), 
    UNIQUE (name), 
    CONSTRAINT unique_parent_id UNIQUE (parent_id)
);

CREATE TYPE type_prices AS ENUM ('WHOLESALE', 'RETAIL');

CREATE TABLE client (
    name VARCHAR(256) NOT NULL, 
    type_prices type_prices, 
    email_contact VARCHAR(255), 
    description TEXT, 
    comment TEXT, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    UNIQUE (name)
);

CREATE UNIQUE INDEX ix_client_email_contact ON client (email_contact);

CREATE TYPE fueltype AS ENUM ('PETROL', 'DIESEL', 'ELECTRIC');

CREATE TABLE engine (
    name VARCHAR(256) NOT NULL, 
    fuel_type fueltype NOT NULL, 
    power INTEGER, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT name_format CHECK (name ~ '^[a-zA-Z0-9/-]+$'), 
    UNIQUE (name)
);

CREATE TABLE standardsize (
    name VARCHAR(256) NOT NULL, 
    size_type VARCHAR(64), 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id)
);

CREATE TABLE storagelocation (
    name VARCHAR(20) NOT NULL, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT latin_characters_only CHECK (name ~ '^[A-Z0-9]+$'), 
    UNIQUE (name)
);

CREATE TYPE type_air_filter AS ENUM ('PLASTIC_CASE', 'RUBBER_CASE', 'PAPER_CASE');

CREATE TABLE airfilter (
    id INTEGER NOT NULL, 
    type_case type_air_filter, 
    length INTEGER NOT NULL, 
    width INTEGER NOT NULL, 
    height INTEGER NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(id) REFERENCES standardsize (id)
);

CREATE TABLE autopart (
    brand_id INTEGER NOT NULL, 
    oem_number VARCHAR(256) NOT NULL, 
    name VARCHAR(256) NOT NULL, 
    description TEXT, 
    width FLOAT, 
    height FLOAT, 
    length FLOAT, 
    weight FLOAT, 
    purchase_price DECIMAL(10, 2), 
    retail_price DECIMAL(10, 2), 
    wholesale_price DECIMAL(10, 2), 
    multiplicity INTEGER, 
    minimum_balance INTEGER, 
    min_balance_auto BOOLEAN, 
    min_balance_user BOOLEAN, 
    comment TEXT, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(brand_id) REFERENCES brand (id), 
    CONSTRAINT uq_brand_oem_number UNIQUE (brand_id, oem_number)
);

CREATE INDEX ix_autopart_oem_number ON autopart (oem_number);

CREATE TABLE brand_synonyms (
    brand_id INTEGER NOT NULL, 
    synonym_id INTEGER NOT NULL, 
    PRIMARY KEY (brand_id, synonym_id), 
    FOREIGN KEY(brand_id) REFERENCES brand (id), 
    FOREIGN KEY(synonym_id) REFERENCES brand (id), 
    CONSTRAINT unique_brand_synonyms UNIQUE (brand_id, synonym_id)
);

CREATE TABLE cabinfilter (
    id INTEGER NOT NULL, 
    length INTEGER NOT NULL, 
    width INTEGER NOT NULL, 
    height INTEGER NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(id) REFERENCES standardsize (id)
);

CREATE TABLE carmodel (
    brand_id INTEGER NOT NULL, 
    name VARCHAR(56) NOT NULL, 
    year_start VARCHAR, 
    year_end VARCHAR, 
    description VARCHAR, 
    image VARCHAR(1056), 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT name_format CHECK (name ~ '^[a-zA-Z0-9/-]+$'), 
    FOREIGN KEY(brand_id) REFERENCES brand (id), 
    CONSTRAINT unique_brand_name UNIQUE (brand_id, name), 
    UNIQUE (image), 
    UNIQUE (name)
);

CREATE TABLE customer (
    id INTEGER NOT NULL, 
    email_outgoing_price VARCHAR(255), 
    PRIMARY KEY (id), 
    FOREIGN KEY(id) REFERENCES client (id), 
    UNIQUE (id)
);

CREATE UNIQUE INDEX ix_customer_email_outgoing_price ON customer (email_outgoing_price);

CREATE TABLE provider (
    id INTEGER NOT NULL, 
    email_incoming_price VARCHAR(255), 
    PRIMARY KEY (id), 
    FOREIGN KEY(id) REFERENCES client (id), 
    UNIQUE (id)
);

CREATE UNIQUE INDEX ix_provider_email_incoming_price ON provider (email_incoming_price);

CREATE TABLE sealsize (
    id INTEGER NOT NULL, 
    inner_diameter INTEGER NOT NULL, 
    external_diameter INTEGER NOT NULL, 
    width INTEGER NOT NULL, 
    width_with_projection INTEGER, 
    PRIMARY KEY (id), 
    FOREIGN KEY(id) REFERENCES standardsize (id)
);

CREATE TABLE autopart_category_association (
    autopart_id INTEGER, 
    category_id INTEGER, 
    PRIMARY KEY (autopart_id, category_id), 
    FOREIGN KEY(autopart_id) REFERENCES autopart (id), 
    FOREIGN KEY(category_id) REFERENCES category (id), 
    CONSTRAINT unique_autopart_category UNIQUE (autopart_id, category_id)
);

CREATE TABLE autopart_storage_association (
    autopart_id INTEGER, 
    storage_location_id INTEGER, 
    FOREIGN KEY(autopart_id) REFERENCES autopart (id), 
    FOREIGN KEY(storage_location_id) REFERENCES storagelocation (id), 
    CONSTRAINT unique_autopart_storage_location UNIQUE (autopart_id, storage_location_id)
);

CREATE TABLE car_model_autopart_association (
    carmodel_id INTEGER, 
    autopart_id INTEGER, 
    FOREIGN KEY(autopart_id) REFERENCES autopart (id), 
    FOREIGN KEY(carmodel_id) REFERENCES carmodel (id), 
    CONSTRAINT unique_carmodel_autopart UNIQUE (carmodel_id, autopart_id)
);

CREATE TABLE car_model_engine_association (
    carmodel_id INTEGER, 
    engine_id INTEGER, 
    FOREIGN KEY(carmodel_id) REFERENCES carmodel (id), 
    FOREIGN KEY(engine_id) REFERENCES engine (id), 
    CONSTRAINT unique_carmodel_engine UNIQUE (carmodel_id, engine_id)
);

CREATE TABLE customerpricelist (
    date DATE, 
    customer_id INTEGER, 
    is_active BOOLEAN, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(customer_id) REFERENCES customer (id)
);

CREATE TABLE photo (
    url VARCHAR(1056) NOT NULL, 
    autopart_id INTEGER, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(autopart_id) REFERENCES autopart (id), 
    UNIQUE (url), 
    CONSTRAINT unique_photo UNIQUE (url, autopart_id)
);

CREATE TABLE pricelist (
    date DATE, 
    provider_id INTEGER, 
    is_active BOOLEAN, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(provider_id) REFERENCES provider (id)
);

CREATE TABLE standard_size_autopart_association (
    standard_size_id INTEGER, 
    autopart_id INTEGER, 
    FOREIGN KEY(autopart_id) REFERENCES autopart (id), 
    FOREIGN KEY(standard_size_id) REFERENCES standardsize (id), 
    CONSTRAINT unique_standard_size_autopart UNIQUE (standard_size_id, autopart_id)
);

CREATE TABLE customer_price_list_autopart_association (
    customerpricelist_id INTEGER, 
    autopart_id INTEGER, 
    quantity INTEGER NOT NULL, 
    price DECIMAL(10, 2), 
    FOREIGN KEY(autopart_id) REFERENCES autopart (id), 
    FOREIGN KEY(customerpricelist_id) REFERENCES customerpricelist (id)
);

CREATE INDEX ix_customer_price_list_autopart_id ON customer_price_list_autopart_association (autopart_id);

CREATE INDEX ix_customer_price_list_customerpricelist_id ON customer_price_list_autopart_association (customerpricelist_id);

CREATE TABLE price_list_autopart_association (
    pricelist_id INTEGER, 
    autopart_id INTEGER, 
    quantity INTEGER NOT NULL, 
    price DECIMAL(10, 2), 
    FOREIGN KEY(autopart_id) REFERENCES autopart (id), 
    FOREIGN KEY(pricelist_id) REFERENCES pricelist (id)
);

CREATE INDEX ix_price_list_autopart_id ON price_list_autopart_association (autopart_id);

CREATE INDEX ix_price_list_pricelist_id ON price_list_autopart_association (pricelist_id);

INSERT INTO alembic_version (version_num) VALUES ('3e7844c76e31') RETURNING alembic_version.version_num;

-- Running upgrade 3e7844c76e31 -> 5779db2be4c0

CREATE TABLE brand (
    name VARCHAR(256) NOT NULL, 
    country_of_origin VARCHAR(100), 
    website VARCHAR(1056), 
    description TEXT, 
    logo VARCHAR(1056), 
    main_brand BOOLEAN, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_name_brand CHECK (name ~ '^[a-zA-Z0-9]+$'), 
    UNIQUE (name)
);

CREATE TABLE autopart (
    brand_id INTEGER NOT NULL, 
    oem_number VARCHAR(256) NOT NULL, 
    name VARCHAR(256) NOT NULL, 
    description TEXT, 
    width FLOAT, 
    height FLOAT, 
    length FLOAT, 
    weight FLOAT, 
    purchase_price DECIMAL(10, 2), 
    retail_price DECIMAL(10, 2), 
    wholesale_price DECIMAL(10, 2), 
    multiplicity INTEGER, 
    minimum_balance INTEGER, 
    min_balance_auto BOOLEAN, 
    min_balance_user BOOLEAN, 
    comment TEXT, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(brand_id) REFERENCES brand (id), 
    CONSTRAINT uq_brand_oem_number UNIQUE (brand_id, oem_number)
);

CREATE INDEX ix_autopart_oem_number ON autopart (oem_number);

CREATE TABLE brand_synonyms (
    brand_id INTEGER NOT NULL, 
    synonym_id INTEGER NOT NULL, 
    PRIMARY KEY (brand_id, synonym_id), 
    FOREIGN KEY(brand_id) REFERENCES brand (id), 
    FOREIGN KEY(synonym_id) REFERENCES brand (id), 
    CONSTRAINT unique_brand_synonyms UNIQUE (brand_id, synonym_id)
);

CREATE TABLE photo (
    url VARCHAR(1056) NOT NULL, 
    autopart_id INTEGER, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(autopart_id) REFERENCES autopart (id), 
    UNIQUE (url), 
    CONSTRAINT unique_photo UNIQUE (url, autopart_id)
);

UPDATE alembic_version SET version_num='5779db2be4c0' WHERE alembic_version.version_num = '3e7844c76e31';

-- Running upgrade 5779db2be4c0 -> b125d65c004a

CREATE TABLE brand (
    name VARCHAR(256) NOT NULL, 
    country_of_origin VARCHAR(100), 
    website VARCHAR(1056), 
    description TEXT, 
    logo VARCHAR(1056), 
    main_brand BOOLEAN, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT check_name_brand CHECK (name ~ '^[a-zA-Z0-9]+$'), 
    UNIQUE (name)
);

CREATE TABLE autopart (
    brand_id INTEGER NOT NULL, 
    oem_number VARCHAR(256) NOT NULL, 
    name VARCHAR(256) NOT NULL, 
    description TEXT, 
    width FLOAT, 
    height FLOAT, 
    length FLOAT, 
    weight FLOAT, 
    purchase_price DECIMAL(10, 2), 
    retail_price DECIMAL(10, 2), 
    wholesale_price DECIMAL(10, 2), 
    multiplicity INTEGER, 
    minimum_balance INTEGER, 
    min_balance_auto BOOLEAN, 
    min_balance_user BOOLEAN, 
    comment TEXT, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(brand_id) REFERENCES brand (id), 
    CONSTRAINT uq_brand_oem_number UNIQUE (brand_id, oem_number)
);

CREATE INDEX ix_autopart_oem_number ON autopart (oem_number);

CREATE TABLE brand_synonyms (
    brand_id INTEGER NOT NULL, 
    synonym_id INTEGER NOT NULL, 
    PRIMARY KEY (brand_id, synonym_id), 
    FOREIGN KEY(brand_id) REFERENCES brand (id), 
    FOREIGN KEY(synonym_id) REFERENCES brand (id), 
    CONSTRAINT unique_brand_synonyms UNIQUE (brand_id, synonym_id)
);

CREATE TABLE photo (
    url VARCHAR(1056) NOT NULL, 
    autopart_id INTEGER, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(autopart_id) REFERENCES autopart (id), 
    UNIQUE (url), 
    CONSTRAINT unique_photo UNIQUE (url, autopart_id)
);

UPDATE alembic_version SET version_num='b125d65c004a' WHERE alembic_version.version_num = '5779db2be4c0';

-- Running upgrade b125d65c004a -> 197a50163aef

ALTER TABLE brand_synonyms ADD CONSTRAINT unique_brand_synonyms_2 UNIQUE (brand_id, synonym_id);

UPDATE alembic_version SET version_num='197a50163aef' WHERE alembic_version.version_num = 'b125d65c004a';

-- Running upgrade 197a50163aef -> 6eef4f3b5048

ALTER TABLE brand_synonyms DROP CONSTRAINT unique_brand_synonyms_2;

UPDATE alembic_version SET version_num='6eef4f3b5048' WHERE alembic_version.version_num = '197a50163aef';

-- Running upgrade 6eef4f3b5048 -> f031465ac9e8

ALTER TABLE brand DROP CONSTRAINT check_name_brand;

ALTER TABLE brand ADD CONSTRAINT check_name_brand CHECK (name ~ '^[a-zA-Z0-9-]+$');

UPDATE alembic_version SET version_num='f031465ac9e8' WHERE alembic_version.version_num = '6eef4f3b5048';

-- Running upgrade f031465ac9e8 -> 6ecf1f115624

ALTER TABLE brand DROP CONSTRAINT check_name_brand;

ALTER TABLE brand ADD CONSTRAINT check_name_brand CHECK (name ~ '^[a-zA-Z0-9]+(?:[ -]?[a-zA-Z0-9]+)*$');

UPDATE alembic_version SET version_num='6ecf1f115624' WHERE alembic_version.version_num = 'f031465ac9e8';

-- Running upgrade 6ecf1f115624 -> ceb8b4924fce

ALTER TABLE brand_synonyms ADD CONSTRAINT unique_brand_synonyms_v2 UNIQUE (brand_id, synonym_id);

UPDATE alembic_version SET version_num='ceb8b4924fce' WHERE alembic_version.version_num = '6ecf1f115624';

-- Running upgrade ceb8b4924fce -> 102fc3bf7eb3

ALTER TABLE brand_synonyms DROP CONSTRAINT unique_brand_synonyms_v2;

ALTER TABLE brand_synonyms ADD CONSTRAINT unique_brand_synonyms_v3 UNIQUE (brand_id, synonym_id);

ALTER TABLE brand_synonyms DROP CONSTRAINT brand_synonyms_brand_id_fkey;

ALTER TABLE brand_synonyms DROP CONSTRAINT brand_synonyms_synonym_id_fkey;

ALTER TABLE brand_synonyms ADD FOREIGN KEY(brand_id) REFERENCES brand (id) ON DELETE CASCADE;

ALTER TABLE brand_synonyms ADD FOREIGN KEY(synonym_id) REFERENCES brand (id) ON DELETE CASCADE;

UPDATE alembic_version SET version_num='102fc3bf7eb3' WHERE alembic_version.version_num = 'ceb8b4924fce';

-- Running upgrade 102fc3bf7eb3 -> 04da60905cf7

ALTER TABLE brand_synonyms DROP CONSTRAINT unique_brand_synonyms_v3;

ALTER TABLE brand_synonyms ADD CONSTRAINT unique_brand_synonyms_v4 UNIQUE (brand_id, synonym_id);

UPDATE alembic_version SET version_num='04da60905cf7' WHERE alembic_version.version_num = '102fc3bf7eb3';

-- Running upgrade 04da60905cf7 -> 3861057a6ecd

ALTER TABLE autopart ADD COLUMN barcode VARCHAR(256) NOT NULL;

ALTER TABLE autopart ADD UNIQUE (barcode);

ALTER TABLE brand_synonyms DROP CONSTRAINT unique_brand_synonyms_v4;

UPDATE alembic_version SET version_num='3861057a6ecd' WHERE alembic_version.version_num = '04da60905cf7';

-- Running upgrade 3861057a6ecd -> 87c207d08bc0

CREATE TABLE category (
    name VARCHAR(256) NOT NULL, 
    parent_id INTEGER, 
    comment TEXT, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    FOREIGN KEY(parent_id) REFERENCES category (id), 
    UNIQUE (name)
);

CREATE TABLE storagelocation (
    name VARCHAR(20) NOT NULL, 
    id SERIAL NOT NULL, 
    PRIMARY KEY (id), 
    CONSTRAINT latin_characters_only CHECK (name ~ '^[A-Z0-9]+$'), 
    UNIQUE (name)
);

CREATE TABLE autopart_category_association (
    autopart_id INTEGER NOT NULL, 
    category_id INTEGER NOT NULL, 
    PRIMARY KEY (autopart_id, category_id), 
    FOREIGN KEY(autopart_id) REFERENCES autopart (id), 
    FOREIGN KEY(category_id) REFERENCES category (id), 
    CONSTRAINT unique_autopart_category UNIQUE (autopart_id, category_id)
);

CREATE TABLE autopart_storage_association (
    autopart_id INTEGER NOT NULL, 
    storage_location_id INTEGER NOT NULL, 
    FOREIGN KEY(autopart_id) REFERENCES autopart (id), 
    FOREIGN KEY(storage_location_id) REFERENCES storagelocation (id), 
    CONSTRAINT unique_autopart_storage_location UNIQUE (autopart_id, storage_location_id)
);

UPDATE alembic_version SET version_num='87c207d08bc0' WHERE alembic_version.version_num = '3861057a6ecd';

-- Running upgrade 87c207d08bc0 -> 2f1149ea23e6

ALTER TABLE storagelocation DROP CONSTRAINT latin_characters_only;

ALTER TABLE storagelocation ADD CONSTRAINT latin_characters_only CHECK (name ~ '^[A-Z0-9 ]+$');

UPDATE alembic_version SET version_num='2f1149ea23e6' WHERE alembic_version.version_num = '87c207d08bc0';

COMMIT;


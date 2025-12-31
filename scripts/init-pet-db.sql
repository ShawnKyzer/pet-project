-- Pet Registration Database Schema
-- This is the source system containing pet registration data

CREATE TABLE IF NOT EXISTS pet_owners (
    owner_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    phone VARCHAR(20),
    address VARCHAR(500),
    city VARCHAR(100),
    state VARCHAR(50),
    zip_code VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pets (
    pet_id SERIAL PRIMARY KEY,
    owner_id INTEGER REFERENCES pet_owners(owner_id),
    name VARCHAR(100) NOT NULL,
    species VARCHAR(50) NOT NULL,  -- dog, cat, bird, etc.
    breed VARCHAR(100),
    birth_date DATE,
    weight_kg DECIMAL(5,2),
    collar_id VARCHAR(50) UNIQUE NOT NULL,  -- Links to sensor collar
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_pets_collar_id ON pets(collar_id);
CREATE INDEX idx_pets_owner_id ON pets(owner_id);

-- Insert sample pet owners
INSERT INTO pet_owners (first_name, last_name, email, phone, city, state, zip_code) VALUES
('John', 'Smith', 'john.smith@email.com', '555-0101', 'San Francisco', 'CA', '94102'),
('Sarah', 'Johnson', 'sarah.j@email.com', '555-0102', 'Los Angeles', 'CA', '90001'),
('Mike', 'Williams', 'mike.w@email.com', '555-0103', 'Seattle', 'WA', '98101'),
('Emily', 'Brown', 'emily.b@email.com', '555-0104', 'Portland', 'OR', '97201'),
('David', 'Jones', 'david.j@email.com', '555-0105', 'Denver', 'CO', '80201'),
('Lisa', 'Garcia', 'lisa.g@email.com', '555-0106', 'Austin', 'TX', '78701'),
('James', 'Miller', 'james.m@email.com', '555-0107', 'Phoenix', 'AZ', '85001'),
('Jennifer', 'Davis', 'jennifer.d@email.com', '555-0108', 'San Diego', 'CA', '92101'),
('Robert', 'Martinez', 'robert.m@email.com', '555-0109', 'Las Vegas', 'NV', '89101'),
('Amanda', 'Anderson', 'amanda.a@email.com', '555-0110', 'Miami', 'FL', '33101');

-- Insert sample pets with collar IDs
INSERT INTO pets (owner_id, name, species, breed, birth_date, weight_kg, collar_id) VALUES
(1, 'Max', 'dog', 'Golden Retriever', '2020-03-15', 32.5, 'COLLAR-001'),
(1, 'Bella', 'cat', 'Persian', '2021-06-20', 4.2, 'COLLAR-002'),
(2, 'Charlie', 'dog', 'Labrador', '2019-11-08', 29.8, 'COLLAR-003'),
(2, 'Luna', 'cat', 'Siamese', '2022-01-12', 3.8, 'COLLAR-004'),
(3, 'Cooper', 'dog', 'Beagle', '2021-04-25', 12.3, 'COLLAR-005'),
(4, 'Daisy', 'dog', 'Poodle', '2020-08-30', 8.5, 'COLLAR-006'),
(4, 'Milo', 'cat', 'Maine Coon', '2019-12-05', 7.2, 'COLLAR-007'),
(5, 'Rocky', 'dog', 'German Shepherd', '2018-07-14', 38.0, 'COLLAR-008'),
(6, 'Sadie', 'dog', 'Bulldog', '2021-02-28', 22.5, 'COLLAR-009'),
(6, 'Oliver', 'cat', 'British Shorthair', '2020-10-18', 5.5, 'COLLAR-010'),
(7, 'Tucker', 'dog', 'Husky', '2019-09-22', 27.0, 'COLLAR-011'),
(8, 'Chloe', 'cat', 'Ragdoll', '2022-03-08', 4.8, 'COLLAR-012'),
(8, 'Bear', 'dog', 'Rottweiler', '2020-01-30', 45.0, 'COLLAR-013'),
(9, 'Penny', 'dog', 'Corgi', '2021-07-16', 11.2, 'COLLAR-014'),
(10, 'Zeus', 'dog', 'Great Dane', '2020-05-10', 55.0, 'COLLAR-015');

-- Create a view for easy querying
CREATE VIEW pet_registration AS
SELECT
    p.pet_id,
    p.collar_id,
    p.name as pet_name,
    p.species,
    p.breed,
    p.birth_date,
    p.weight_kg,
    o.owner_id,
    o.first_name || ' ' || o.last_name as owner_name,
    o.email as owner_email,
    o.city,
    o.state
FROM pets p
JOIN pet_owners o ON p.owner_id = o.owner_id;

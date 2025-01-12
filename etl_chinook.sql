CREATE DATABASE IF NOT EXISTS SLOTH_CHINOOK;
USE DATABASE SLOTH_CHINOOK;

CREATE WAREHOUSE IF NOT EXISTS SLOTH_CHINOOK_WAREHOUSE;
USE WAREHOUSE SLOTH_CHINOOK_WAREHOUSE;

CREATE SCHEMA IF NOT EXISTS SLOTH_CHINOOK.stages;
CREATE OR REPLACE STAGE my_stage;


CREATE OR REPLACE TABLE employee_staging(
    EmployeeId INT,
    LastName VARCHAR(20),
    FirstName VARCHAR(20),
    Title VARCHAR(30),
    ReportsTo INT,
    BirthDate DATETIME,
    HireDate DATETIME,
    Address VARCHAR(70),
    City VARCHAR(40),
    State VARCHAR(40),
    Country VARCHAR(40),
    PostalCode VARCHAR(10),
    Phone VARCHAR(24),
    Fax VARCHAR(24),
    Email VARCHAR(60)
);

COPY INTO employee_staging
FROM @stages.my_stage/employee.csv
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1);



CREATE OR REPLACE TABLE customer_staging(
    CustomerId INT,
    FirstName VARCHAR(40),
    LastName VARCHAR(20),
    Company VARCHAR(80),
    Address VARCHAR(70),
    City VARCHAR(40),
    State VARCHAR(40),
    Country VARCHAR(40),
    PostalCode VARCHAR(10),
    Phone VARCHAR(24),
    Fax VARCHAR(24),
    Email VARCHAR(60),
    SupportRepId INT
);

COPY INTO customer_staging
FROM @stages.my_stage/customer.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


CREATE OR REPLACE TABLE invoice_staging(
    InvoiceId INT,
    CustomerId INT,
    InvoiceDate DATETIME,
    BillingAddress VARCHAR(70),
    BillingCity VARCHAR(40),
    BillingState VARCHAR(40),
    BillingCountry VARCHAR(40),
    BillingPostalCode VARCHAR(10),
    Total DECIMAL(10,2)
);

COPY INTO invoice_staging
FROM @stages.my_stage/invoice.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


CREATE OR REPLACE TABLE invoiceline_staging(
    InvoiceLineId INT,
    InvoiceId INT,
    TrackId INT,
    UnitPrice DECIMAL(10,2),
    Quantity INT
);



COPY INTO invoiceline_staging
FROM @stages.my_stage/invoiceline.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);




CREATE OR REPLACE TABLE track_staging(
    TrackId INT,
    Name VARCHAR(200),
    AlbumId INT,
    MediaTypeId INT,
    GenreId INT,
    Composer VARCHAR(220),
    Milliseconds INT,
    Bytes INT,
    UnitPrice DECIMAL(10,2)
);

COPY INTO track_staging
FROM @stages.my_stage/track.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);



CREATE OR REPLACE TABLE mediatype_staging(
    MediaTypeId INT,
    Name VARCHAR(120)
);

COPY INTO mediatype_staging
FROM @stages.my_stage/mediatype.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);





CREATE OR REPLACE TABLE genre_staging(
    GenreId INT,
    Name VARCHAR(120)
);

COPY INTO genre_staging
FROM @stages.my_stage/genre.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);



CREATE OR REPLACE TABLE playlist_staging(
    PlaylistId INT,
    Name VARCHAR(120)
);

COPY INTO playlist_staging
FROM @stages.my_stage/playlist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);


CREATE OR REPLACE TABLE playlisttrack_staging(
    PlaylistId INT,
    TrackId INT
);

COPY INTO playlisttrack_staging
FROM @stages.my_stage/playlisttrack.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);



CREATE OR REPLACE TABLE album_staging(
    AlbumId INT,
    Title VARCHAR(160),
    ArtistId INT
);

COPY INTO album_staging
FROM @stages.my_stage/album.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);



CREATE OR REPLACE TABLE artist_staging(
    ArtistId INT,
    Name VARCHAR(120)
);

COPY INTO artist_staging
FROM @stages.my_stage/artist.csv
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' SKIP_HEADER = 1);







CREATE OR REPLACE TABLE dm_date AS
SELECT
    ROW_NUMBER() OVER (ORDER BY unique_dates.InvoiceDate) AS iddm_date,
    EXTRACT(WEEK FROM unique_dates.InvoiceDate) AS week,
    EXTRACT(YEAR FROM unique_dates.InvoiceDate) AS year,
    EXTRACT(DAY FROM unique_dates.InvoiceDate) AS day,
    EXTRACT(MONTH FROM unique_dates.InvoiceDate) AS month,
    unique_dates.InvoiceDate AS full_date
FROM (
    SELECT DISTINCT CAST(InvoiceDate AS DATE) AS InvoiceDate
    FROM invoice_staging
) unique_dates;




CREATE OR REPLACE TABLE dm_address AS
SELECT
    ROW_NUMBER() OVER (ORDER BY BillingAddress, BillingState, BillingCity) AS id_address,
    BillingAddress AS street,
    BillingState AS state,
    BillingCity AS city
FROM (
    SELECT DISTINCT
        BillingAddress,
        BillingState,
        BillingCity
    FROM invoice_staging
);





CREATE OR REPLACE TABLE dm_customer AS
SELECT
    CustomerId AS iddm_customer,
    City AS city,
FROM customer_staging;



CREATE OR REPLACE TABLE dm_tracks AS
SELECT
    ts.TrackId AS iddm_tracks,
    ts.Name AS name,
    ts.Composer AS composer,
    ts.Milliseconds AS milliseconds,
    ts.Bytes AS bytes,
    al.Title AS album,
    an.Name AS artist_name,
    mt.Name AS media_type,
    gn.Name AS genre_name,
FROM track_staging ts
 JOIN album_staging al ON ts.AlbumId = al.AlbumId
 JOIN artist_staging an ON al.ArtistId = an.ArtistId
 JOIN mediatype_staging mt ON ts.MediaTypeId = mt.MediaTypeId
 JOIN genre_staging gn ON ts.GenreId = gn.GenreId;




CREATE OR REPLACE TABLE facts_invoice AS
SELECT
    ROW_NUMBER() OVER (ORDER BY invoice_staging.InvoiceId) AS id_invoice, 
     invoiceline_staging.UnitPrice AS price, 
    invoiceline_staging.Quantity AS quantity, 
    invoice_staging.Total AS total,  
    customer_staging.CustomerId AS dm_customer_iddm_customer,
    track_staging.TrackId AS dm_tracks_iddm_tracks,
    dm_address.id_address AS dm_address_iddm_address, 
    dm_date.iddm_date AS dm_date_iddm_date,
FROM invoice_staging
JOIN invoiceline_staging ON invoice_staging.InvoiceId = invoiceline_staging.InvoiceId 
JOIN customer_staging  ON invoice_staging.CustomerId = customer_staging.CustomerId 
JOIN track_staging  ON invoiceline_staging.TrackId = track_staging.TrackId 
JOIN dm_address  ON invoice_staging.BillingAddress = dm_address.street
JOIN dm_date ON CAST(invoice_staging.InvoiceDate AS DATE) = dm_date.full_date;





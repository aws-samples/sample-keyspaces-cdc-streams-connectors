# Popular Movies Dataset Example

This example demonstrates how to use the S3 Vector Connector with a movie dataset from The Movie Database (TMDB). The dataset contains popular movies with their metadata including titles, overviews, release dates, popularity scores, and ratings.

## Dataset Overview

The `movies_list.csv` file contains 9,282 movie records with the following structure:

| Column | Type | Description |
|--------|------|-------------|
| title | text | Movie title |
| overview | text | Movie plot summary |
| original_lang | text | Original language code (e.g., 'en' for English) |
| rel_date | date | Release date in YYYY-MM-DD format |
| popularity | decimal | TMDB popularity score |
| vote_count | int | Number of votes received |
| vote_average | decimal | Average rating (0-10 scale) |

## Cassandra Keyspace and Table Schema

### Create Keyspace
```cql
CREATE KEYSPACE IF NOT EXISTS movies
WITH REPLICATION = {
    'class': 'SimpleStrategy',
    'replication_factor': 3
};
```

### Create Table
```cql

CREATE TABLE IF NOT EXISTS movies.movies (
    title text,
    overview text,
    original_lang text,
    rel_date date,
    popularity decimal,
    vote_count int,
    vote_average decimal,
    PRIMARY KEY (title, rel_date)
);
```

## Sample Data Insert Statements

To populate the table run the data_loader.py script


## Usage with S3 Vector Connector

This dataset can be used to test the S3 Vector Connector functionality:

1. **Upload the CSV file** to your S3 bucket
2. **Configure the connector** to read from the S3 location
3. **Set up the Cassandra table** using the schema above
4. **Run the connector** to stream the data from S3 to Cassandra

## Data Characteristics

- **Total Records**: 9,312 movies
- **Date Range**: Movies from various years (primarily 2010s-2020s)
- **Languages**: Primarily English ('en') with some international films
- **Popularity Range**: Wide range from low to very high popularity scores
- **Rating Range**: Vote averages typically between 4.0-8.5


CREATE TABLE authors (
    id SERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE books (
    id SERIAL PRIMARY KEY,
    title TEXT NOT NULL,
    author_id INTEGER NOT NULL,
    genre TEXT,
    base_price REAL,
    FOREIGN KEY (author_id) REFERENCES authors(id)
);

CREATE TABLE book_statistics (
    book_id SERIAL PRIMARY KEY,
    revenue REAL DEFAULT 0,
    expenses REAL DEFAULT 0,
    profit REAL DEFAULT 0,
    stock INTEGER DEFAULT 0
);

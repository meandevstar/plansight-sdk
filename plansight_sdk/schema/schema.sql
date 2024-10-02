CREATE TABLE IF NOT EXISTS Users (
    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL CHECK(length(name) <= 100),
    email TEXT NOT NULL UNIQUE CHECK(length(email) <= 100),
    created_at DATETIME NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_users_name ON Users(name);
CREATE INDEX IF NOT EXISTS idx_users_email ON Users(email);
CREATE INDEX IF NOT EXISTS idx_users_created_at ON Users(created_at);

CREATE TABLE IF NOT EXISTS Activities (
    activity_id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    activity_type TEXT NOT NULL CHECK(length(activity_type) <= 50),
    timestamp DATETIME NOT NULL,
    metadata TEXT CHECK(length(metadata) <= 255),
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);

CREATE INDEX IF NOT EXISTS idx_activities_user_id ON Activities(user_id);
CREATE INDEX IF NOT EXISTS idx_activities_activity_type ON Activities(activity_type);
CREATE INDEX IF NOT EXISTS idx_activities_timestamp ON Activities(timestamp);

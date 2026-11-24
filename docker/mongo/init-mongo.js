// MongoDB initialization script
// Runs on first container start to create database, collections, and indexes

// Switch to the bigdata database
db = db.getSiblingDB("bigdata");

// --- Collection: observations ---
// Stores individual processed butterfly observations (batch + streaming)
db.createCollection("observations");

db.observations.createIndex(
  { observation_id: 1 },
  { unique: true, name: "idx_observation_id_unique" }
);
db.observations.createIndex(
  { county: 1, observed_on: 1 },
  { name: "idx_county_date" }
);
db.observations.createIndex(
  { observed_on: 1 },
  { name: "idx_observed_on" }
);

print("Created collection 'observations' with indexes.");

// --- Collection: monthly_statistics ---
// Stores monthly observation counts and average temperatures per county
db.createCollection("monthly_statistics");

db.monthly_statistics.createIndex(
  { county: 1, year: 1, month: 1 },
  { unique: true, name: "idx_county_year_month_unique" }
);

print("Created collection 'monthly_statistics' with indexes.");

// --- Collection: correlations ---
// Stores county, year, and overall temperature-observation correlations
db.createCollection("correlations");
db.correlations.createIndex(
  { group_type: 1, group_value: 1 },
  { unique: true, name: "idx_group_type_value_unique" }
);

print("Created collection 'correlations' with indexes.");

// --- Collection: realtime_statistics ---
// Stores latest streaming metrics and recent observations per county
db.createCollection("realtime_statistics");

db.realtime_statistics.createIndex(
  { county: 1 },
  { unique: true, name: "idx_county_unique" }
);
db.realtime_statistics.createIndex(
  { last_updated: 1 },
  { name: "idx_last_updated" }
);

print("Created collection 'realtime_statistics' with indexes.");

print("MongoDB initialization completed successfully.");
print("Database: bigdata");
print("Collections: observations, monthly_statistics, correlations, realtime_statistics");

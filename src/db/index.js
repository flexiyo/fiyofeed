import postgres from "postgres";
import cassandra from "cassandra-driver";
import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

const cloud = {
  secureConnectBundle: process.env.FIYOCDB_SECURE_BUNDLE_PATH,
};

const authProvider = new cassandra.auth.PlainTextAuthProvider(
  "token",
  process.env.FIYOCDB_APPLICATION_TOKEN
);

const cassandraClient = new cassandra.Client({ cloud, authProvider });
const redisClient = new Redis(process.env.FIYORDB_URI);

let pgClient;

const connectDB = async () => {
  if (!pgClient) {
    try {
      pgClient = postgres(process.env.FIYOPGDB_URI);

      await cassandraClient.connect();

      await redisClient.ping();

      console.log("Databases connected successfully");
    } catch (error) {
      console.error("Database connection error:", error);
      throw new Error("Failed to connect to the database");
    }
  }
  return pgClient;
};

async function sql(strings, ...values) {
  return pgClient(strings, ...values);
}

async function cql(strings, ...values) {
  const query = strings.join("?");
  return cassandraClient.execute(query, values, { prepare: true });
}

async function rdb(command, ...args) {
  return redisClient[command](...args);
}

export { sql, cql, rdb, connectDB };

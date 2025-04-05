import postgres from "postgres";
import Redis from "ioredis";
import dotenv from "dotenv";

dotenv.config();

const redisClient = new Redis(process.env.FIYORDB_URI);

let pgClient;

const connectDB = async () => {
  if (!pgClient) {
    try {
      pgClient = postgres(process.env.FIYOPGDB_URI);

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
  return pgClient.unsafe(strings, ...values);
}

async function rdb(command, ...args) {
  return redisClient[command](...args);
}

export { sql, rdb, connectDB };

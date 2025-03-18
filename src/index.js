import grpc from "@grpc/grpc-js";
import protoLoader from "@grpc/proto-loader";
import axios from "axios";
import fs from "fs/promises";
import path from "path";
import dotenv from "dotenv";
import { connectDB } from "./db/index.js";
import { GrpcResponse } from "./utils/GrpcResponse.js";
import contentService from "./services/content.service.js";
import feedService from "./services/feed.service.js";

dotenv.config();

const PORT = process.env.PORT || 8002;

const PROTO_DIR = path.resolve("./src/proto");
const BASE_URL = "https://fiyoproto.vercel.app/fiyofeed";
const GRPC_SECRET = process.env.GRPC_SECRET;

const loadProto = async (name) => {
  try {
    const filePath = path.join(PROTO_DIR, name);
    await fs.mkdir(PROTO_DIR, { recursive: true });
    const { data } = await axios.get(`${BASE_URL}/${name}`);
    await fs.writeFile(filePath, data);

    return grpc.loadPackageDefinition(
      await protoLoader.load(filePath, {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true,
      })
    );
  } catch (error) {
    console.error(`❌ Error loading proto file '${name}':`, error);
    throw error;
  }
};

const authenticateInterceptor = (call, callback, next) => {
  const token = call.metadata.get("authorization")?.[0];
  if (token === GRPC_SECRET) return next();
  callback(null, GrpcResponse.error("Unauthorized request to 'fiyofeed'."));
};

const wrapService = (service) =>
  Object.fromEntries(
    Object.entries(service).map(([method, handler]) => [
      method,
      (call, callback) =>
        authenticateInterceptor(call, callback, () => handler(call, callback)),
    ])
  );

const startServer = async () => {
  try {
    await loadProto("common.proto");
    const [contentProto, feedProto] = await Promise.all(
      ["content.proto", "feed.proto"].map(loadProto)
    );

    const server = new grpc.Server();
    server.addService(
      contentProto.content.ContentService.service,
      wrapService(contentService)
    );
    server.addService(
      feedProto.feed.FeedService.service,
      wrapService(feedService)
    );

    const credentials = grpc.ServerCredentials.createInsecure();
    // const credentials = grpc.ServerCredentials.createSsl();

    server.bindAsync(`0.0.0.0:${PORT}`, credentials, (err, boundPort) => {
      if (err) {
        console.error("❌ gRPC binding error:", err);
        process.exit(1);
      }

      console.log(`🚀 gRPC Server running on port ${boundPort}`);
    });
  } catch (error) {
    console.error("❌ Error starting server:", error);
    process.exit(1);
  }
};

connectDB()
  .then(startServer)
  .catch((err) => {
    console.error("❌ Database connection error:", err);
    process.exit(1);
  });

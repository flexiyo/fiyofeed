import { rdb } from "../db/index.js";
import { GrpcResponse } from "../utils/GrpcResponse.js";
import { getContentById } from "../handlers/content.handler.js";
import { getStarterFeed, precomputeFeed } from "../handlers/feed.handler.js";

const feedService = {
  GetUserFeed: async (call, callback) => {
    try {
      const { req_user_id, table_name } = call.request;

      let feedContentIds = await rdb(
        "get",
        `feed:${req_user_id}:${table_name}`
      );

      if (!feedContentIds || feedContentIds.length < 0) {
        feedContentIds = await getStarterFeed(req_user_id, table_name);
      }

      if(feedContentIds.length <= 5) {
        precomputeFeed(req_user_id, table_name);
      }

      const contents = [];

      for (const contentId of feedContentIds) {
        const content = await getContentById(contentId, table_name);
        if (content) {
          contents.push(content);
        } else {
          await rdb("del", `feed:${req_user_id}:${table_name}:${contentId}`);
          continue;
        }
      }

      return callback(null, {
        ...GrpcResponse.success(`${table_name.slice(0, -1)} found.`),
        contents,
      });
    } catch (error) {
      console.error("Error in GetUserFeed:", error);
      return callback(GrpcResponse.error("Error in GetUserFeed."));
    }
  }
};

export default feedService;

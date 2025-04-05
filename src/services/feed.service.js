import { rdb } from "../db/index.js";
import { GrpcResponse } from "../utils/GrpcResponse.js";
import { getContentById } from "../handlers/content.handler.js";
import { getStarterFeed, precomputeFeed } from "../handlers/feed.handler.js";

const feedService = {
  GetUserFeed: async (call, callback) => {
    try {
      const { req_user_id, table_name } = call.request;

      console.log(call.request);

      let feedContentIds = await rdb(
        "get",
        `feed:${req_user_id}:${table_name}`
      );

      if (!feedContentIds || feedContentIds.length < 0) {
        feedContentIds = await getStarterFeed(req_user_id, table_name);
      }

      if (feedContentIds.length <= 5) {
        precomputeFeed(req_user_id, table_name);
      }

      let contents = [];

      for (const contentId of feedContentIds) {
        const content = await getContentById(contentId, table_name);
        if (content) {
          contents.push(content);
        } else {
          await rdb("del", `feed:${req_user_id}:${table_name}:${contentId}`);
          continue;
        }
      }

      console.log(contents.length)

      if(contents.length < 0) {
        return callback(null, GrpcResponse.error("Contents not found."))
      }

      return callback(null, {
        ...GrpcResponse.success("Contents found."),
        contents,
      });
    } catch (error) {
      console.error("Error in GetUserFeed:", error);
      return callback(GrpcResponse.error("Error in GetUserFeed."));
    }
  },
};

export default feedService;

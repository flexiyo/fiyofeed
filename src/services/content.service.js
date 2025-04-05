import { v4 as uuidv4 } from "uuid";
import { sql } from "../db/index.js";
import { GrpcResponse } from "../utils/GrpcResponse.js";
import {
  getContentById,
  getContentsByUserId,
} from "../handlers/content.handler.js";

const contentService = {
  GetContent: async (call, callback) => {
    try {
      const { content_id, table_name } = call.request;
      const content = await getContentById(content_id, table_name);

      if (!content)
        return callback(null, GrpcResponse.error("Content not found."));

      return callback(null, {
        ...GrpcResponse.success("Content found."),
        content,
      });
    } catch (error) {
      console.error("Error in GetContent:", error);
      return callback(GrpcResponse.error("Error in GetContent."));
    }
  },
  GetContents: async (call, callback) => {
    try {
      const { content_ids, table_name } = call.request;

      const contents = await getContentsByUserId(content_ids, table_name);

      return callback(null, {
        ...GrpcResponse.success("Content found."),
        contents,
      });
    } catch (error) {
      console.error("Error in GetUserContents:", error);
      return callback(GrpcResponse.error("Error in GetUserContents."));
    }
  },
  CreateContent: async (call, callback) => {
    try {
      const {
        req_user_id,
        media_key,
        collabs,
        caption,
        hashtags,
        track,
        table_name,
      } = call.request;

      const contentId = uuidv4();

      await sql`
        INSERT INTO ${table_name} (id, user_id, media_key, collabs, caption, hashtags, track)
        VALUES (${contentId}, ${req_user_id}, ${media_key}, ${collabs}, ${caption}, ${hashtags}, ${track})
      `;

      return callback(null, GrpcResponse.success("Content created."));
    } catch (error) {
      console.error("Error in CreateContent:", error);
      return callback(GrpcResponse.error("Error in CreateContent."));
    }
  },
  UpdateContent: async (call, callback) => {
    try {
      const { req_user_id, content_id, table_name, updated_fields } =
        call.request;

      const allowedFields = ["collabs", "caption", "hashtags"];

      const fieldsToUpdate = Object.keys(updated_fields).filter((field) =>
        allowedFields.includes(field)
      );

      await sql`
        UPDATE ${table_name}
        SET ${fieldsToUpdate.map(
          (field) => `${field} = ${updated_fields[field]}`
        )}
        WHERE id = ${content_id} AND user_id = ${req_user_id}
      `;

      return callback(null, {
        ...GrpcResponse.success("Content updated."),
        updated_fields,
      });
    } catch (error) {
      console.error("Error in UpdateContent:", error);
      return callback(GrpcResponse.error("Error in UpdateContent."));
    }
  },
  DeleteContent: async (call, callback) => {
    try {
      const { req_user_id, content_id, table_name } = call.request;

      await sql`DELETE FROM ${table_name} WHERE id = ${content_id} AND user_id = ${req_user_id}`;

      return callback(null, GrpcResponse.success("${table_name} deleted."));
    } catch (error) {
      console.error("Error in DeleteContent:", error);
      return callback(GrpcResponse.error("Error in DeleteContent."));
    }
  },
};

export default contentService;

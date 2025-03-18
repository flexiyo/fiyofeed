import { v4 as uuidv4 } from "uuid";
import { cql, rdb, sql } from "../db/index.js";
import { GrpcResponse } from "../utils/GrpcResponse.js";
import { getContentById } from "../handlers/content.handler.js";

const contentService = {
  GetContent: async (call, callback) => {
    try {
      const { content_id, table_name } = call.request;
      const content = await getContentById(content_id, table_name);

      if (!content)
        return callback(
          null,
          GrpcResponse.error(`${table_name.slice(0, -1)} not found."`)
        );

      return callback(null, {
        ...GrpcResponse.success(`${table_name.slice(0, -1)} found.`),
        content,
      });
    } catch (error) {
      console.error("Error in GetContent:", error);
      return callback(GrpcResponse.error("Error in GetContent."));
    }
  },
  GetUserContents : async (call, callback) => {
    try {
      const { user_id, table_name } = call.request;

      const contents = await getContentsByUserId(req_user_id, table_name);

      return callback(null, {
        ...GrpcResponse.success(`${table_name.slice(0, -1)} found.`),
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
        description,
        hashtags,
        track,
        table_name,
      } = call.request;

      const contentId = uuidv4();

      await sql`
        INSERT INTO ${table_name} (id, user_id, media_key, collabs)
        VALUES (${contentId}, ${req_user_id}, ${media_key}, ${collabs})
      `;

      await cql`
        INSERT INTO ${table_name} (id, caption, description, hashtags, track)
        VALUES (${contentId}, ${caption}, ${description}, ${hashtags}, ${track})
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

      const allowedSqlFields = ["collabs"];

      const allowedCqlFields = ["caption", "description", "hashtags"];

      await sql`
        UPDATE ${table_name}
        SET ${allowedSqlFields.map(
          (field) => `${field} = ${updated_fields[field]}`
        )}
        WHERE id = ${content_id} AND user_id = ${req_user_id}
      `;

      await cql`
        UPDATE ${table_name}
        SET ${allowedCqlFields.map(
          (field) => `${field} = ${updated_fields[field]}`
        )}
        WHERE id = ${content_id}
      `;

      return callback(null, {...GrpcResponse.success("Content updated."), updated_fields});
    } catch (error) {
      console.error("Error in UpdateContent:", error);
      return callback(GrpcResponse.error("Error in UpdateContent."));
    }
  },
  DeleteContent: async (call, callback) => {
    try {
      const { req_user_id, content_id, table_name } = call.request;

      await sql`DELETE FROM ${table_name} WHERE id = ${content_id} AND user_id = ${req_user_id}`;

      return callback(null, GrpcResponse.success(`${table_name} deleted.`));
    } catch (error) {
      console.error("Error in DeleteContent:", error);
      return callback(GrpcResponse.error("Error in DeleteContent."));
    }
  },
};

export default contentService;

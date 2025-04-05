import { sql } from "../db/index.js";

export const getContentById = async (contentId, tableName) => {
  try {
    if (!tableName) throw new Error("Table name is missing");

    const [content] = await sql(
      `
      SELECT c.id, c.user_id, c.media_key, c.collabs, c.created_at, c.caption, c.hashtags, c.track, c.likes_count, c.comments_count, c.shares_count,
             u.full_name as user_full_name, u.username AS user_username, u.avatar AS user_avatar,
      FROM ${sql(tableName)} c
      JOIN users u ON c.user_id = u.id
      WHERE c.id = $1
    `,
      [contentId]
    );

    if (!content) return null;

    const collabs = content.collabs?.length
      ? await sql`SELECT id, full_name, username, avatar FROM users WHERE id = ANY(${content.collabs})`
      : [];

    const {
      id,
      media_key,
      created_at,
      caption,
      hashtags,
      track,
      likes_count,
      comments_count,
      shares_count,
    } = content;

    return {
      id,
      creators: [
        {
          id: content.user_id,
          full_name: content.user_full_name,
          username: content.user_username,
          avatar: content.user_avatar,
        },
        ...collabs,
      ],
      media_key,
      created_at,
      caption,
      hashtags,
      track,
      likes_count,
      comments_count,
      shares_count,
    };
  } catch (error) {
    console.error("Error in getContentById:", error);
    return null;
  }
};

export const getContents = async (contentIds, tableName) => {
  try {
    const contents = await sql`
          SELECT c.id, c.user_id, c.media_key, c.collabs, c.created_at, c.caption, c.hashtags, c.track, c.likes_count, c.comments_count, c.shares_count,
                u.full_name as user_full_name, u.username AS user_username, u.avatar AS user_avatar,
          FROM ${tableName} c
          JOIN users u ON c.user_id = u.id
          WHERE c.id = ANY(${contentIds})
        `;

    if (!contents) return [];

    const structuredContents = [];

    for (const content of contents) {
      const {
        id,
        media_key,
        caption,
        hashtags,
        track,
        likes_count,
        comments_count,
        shares_count,
        created_at,
      } = content;

      structuredContents.push({
        id,
        creators: [
          {
            id: content.user_id,
            full_name: content.user_full_name,
            username: content.user_username,
            avatar: content.user_avatar,
          },
          ...collabs,
        ],
        media_key,
        caption,
        hashtags,
        track,
        likes_count,
        comments_count,
        shares_count,
        created_at,
      });
    }

    return structuredContents;
  } catch (error) {
    console.error("Error in getContentsByUserId:", error);
    return [];
  }
};

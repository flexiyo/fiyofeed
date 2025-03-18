export const getContentById = async (contentId, tableName) => {
  try {
    const [content] = await sql`
          SELECT c.id, c.user_id, c.media_key, c.collabs, c.created_at,
                u.full_name as user_full_name, u.username AS user_username, u.avatar AS user_avatar
          FROM ${tableName} c
          JOIN users u ON c.user_id = u.id
          WHERE c.id = ${contentId}
        `;

    if (!content) return null;

    const [metadata] = await cql`
        SELECT caption, description, hashtags, track, likes_count, comments_count, shares_count 
        FROM ${tableName} WHERE id = ${contentId}
      `;

    if (!metadata) return null;

    const collabs =
      content.collabs?.length > 0
        ? await sql`SELECT id, full_name, username, avatar FROM users WHERE id = ANY(${content.collabs})`
        : [];

    return {
      id: content.id,
      user: {
        id: content.user_id,
        full_name: content.user_full_name,
        username: content.user_username,
        avatar: content.user_avatar,
      },
      media_key: content.media_key,
      collabs,
      created_at: content.created_at,
      ...metadata,
    };
  } catch (error) {
    console.error("Error in getContentId:", error);
    return null;
  }
};

export const getContentsByUserId = async (userId, tableName) => {
  try {
    const [contents] = await sql`
          SELECT c.id, c.user_id, c.media_key, c.collabs, c.created_at,
                u.full_name as user_full_name, u.username AS user_username, u.avatar AS user_avatar
          FROM ${tableName} c
          JOIN users u ON c.user_id = u.id
          WHERE c.user_id = ${userId}
        `;

    for (const content of contents) {
      const [metadata] = await cql`
            SELECT caption, description, hashtags, track, likes_count, comments_count, shares_count 
            FROM ${tableName} WHERE id = ${content.id}
          `;

      if (!metadata) continue;

      content.caption = metadata.caption;
      content.description = metadata.description;
      content.hashtags = metadata.hashtags;
      content.track = metadata.track;
      content.likes_count = metadata.likes_count;
      content.comments_count = metadata.comments_count;
      content.shares_count = metadata.shares_count;
    }

    if (!contents) return [];

    return contents;
  } catch (error) {
    console.error("Error in getContentsByUserId:", error);
    return [];
  }
};

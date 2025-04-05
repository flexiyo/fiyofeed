import { sql, rdb } from "../db/index.js";

async function getUserContext(userId) {
  const [mates, follows, interests, interactions] = await Promise.all([
    sql`
      (SELECT mater_id AS id FROM mates WHERE matee_id = ${userId})
      UNION
      (SELECT matee_id AS id FROM mates WHERE mater_id = ${userId})
    `,
    sql`SELECT followee_id FROM followers WHERE follower_id = ${userId}`,
    sql`SELECT interests FROM users WHERE id = ${userId}`,
    sql`
      SELECT content_id, content_type, action_type, created_at 
      FROM user_interactions 
      WHERE user_id = ${userId} 
      ORDER BY created_at DESC LIMIT 50
    `,
  ]);

  const interactionsByType = {};
  interactions.forEach((item) => {
    if (!interactionsByType[item.action_type]) {
      interactionsByType[item.action_type] = new Set();
    }
    interactionsByType[item.action_type].add(item.content_id);
  });

  Object.keys(interactionsByType).forEach((key) => {
    interactionsByType[key] = [...interactionsByType[key]];
  });

  const likedCreators = new Set();
  const interactedContent = interactions.filter(
    (item) => item.action_type === "like"
  );

  if (interactedContent.length > 0) {
    const contentIds = interactedContent.map((item) => item.content_id);
    const contentTypes = [
      ...new Set(interactedContent.map((item) => item.content_type)),
    ];

    for (const type of contentTypes) {
      const typeIds = contentIds.filter(
        (_, idx) => interactedContent[idx].content_type === type
      );

      if (typeIds.length > 0) {
        const tableName = type === "clips" ? "clips" : "posts";
        const creators = await sql`
          SELECT DISTINCT user_id FROM ${tableName}
          WHERE id = ANY(${typeIds})
        `;

        creators.forEach((creator) => likedCreators.add(creator.user_id));
      }
    }
  }

  return {
    mates: mates.map((m) => m.id),
    follows: follows.map((f) => f.followee_id),
    interests: interests?.[0]?.interest_tags || [],
    networkIds: [
      ...new Set([
        ...mates.map((m) => m.id),
        ...follows.map((f) => f.followee_id),
      ]),
    ],
    interactions: interactionsByType,
    likedCreators: [...likedCreators],
  };
}

async function getTrendingTags(networkIds, timeframe = 7) {
  if (!networkIds.length) return [];

  const networkContent = await sql`
    (SELECT id FROM posts WHERE user_id = ANY(${networkIds}) 
     AND created_at > NOW() - INTERVAL '${sql(timeframe)} days')
    UNION
    (SELECT id FROM clips WHERE user_id = ANY(${networkIds}) 
     AND created_at > NOW() - INTERVAL '${sql(timeframe)} days')
  `;

  const contentIds = networkContent.map((item) => item.id);
  if (!contentIds.length) return [];

  const postIds = contentIds.filter((id) => !id.toString().startsWith("c_"));
  const clipIds = contentIds.filter((id) => id.toString().startsWith("c_"));

  const tagQueries = [];
  if (postIds.length > 0) {
    tagQueries.push(
      sql`SELECT hashtags FROM posts WHERE id =  ANY(${postIds})`
    );
  }
  if (clipIds.length > 0) {
    tagQueries.push(sql`SELECT hashtags FROM clips WHERE id = ANY(${clipIds})`);
  }

  const tagResults = (await Promise.all(tagQueries)).flat();

  const tagCounts = {};
  tagResults.forEach((result) => {
    if (result?.hashtags) {
      result.hashtags.forEach((tag) => {
        tagCounts[tag] = (tagCounts[tag] || 0) + 1;
      });
    }
  });

  return Object.entries(tagCounts)
    .map(([tag, count]) => ({ tag, count }))
    .sort((a, b) => b.count - a.count)
    .slice(0, 10)
    .map((item) => item.tag);
}

async function fetchContent(tableName, strategy, limit = 20) {
  const { userIds, tags, sortBy, timeframe = 7 } = strategy;

  if (userIds && userIds.length > 0) {
    return sql`
      SELECT id, user_id, created_at 
      FROM ${tableName} 
      WHERE user_id = ANY(${userIds})
      AND created_at > NOW() - INTERVAL '${sql(timeframe)} days' 
      LIMIT ${limit}
    `;
  }

  if (tags && tags.length > 0) {
    const timestamp = Date.now() - timeframe * 24 * 60 * 60 * 1000;
    const taggedIds = await sql`
      SELECT id FROM ${tableName} 
      WHERE hashtags && ${tags} 
      AND created_at > toTimestamp(${timestamp}) 
      LIMIT ${limit * 2}
    `;

    if (!taggedIds.length) return [];

    const ids = taggedIds.map((item) => item.id);
    return sql`
      SELECT id, user_id, created_at 
      FROM ${tableName} 
      WHERE id = ANY(${ids})
      LIMIT ${limit}
    `;
  }

  if (sortBy === "popular") {
    const timestamp = Date.now() - timeframe * 24 * 60 * 60 * 1000;
    const popularIds = await sql`
      SELECT id FROM ${tableName} 
      WHERE created_at > toTimestamp(${timestamp})
      ORDER BY likes_count DESC, comments_count DESC 
      LIMIT ${limit * 2}
    `;

    if (!popularIds.length) return [];

    const ids = popularIds.map((item) => item.id);
    return sql`
      SELECT id, user_id, created_at 
      FROM ${tableName} 
      WHERE id = ANY(${ids})
      LIMIT ${limit}
    `;
  }

  return sql`
    SELECT id, user_id, created_at 
    FROM ${tableName} 
    WHERE created_at > NOW() - INTERVAL '${sql(timeframe)} days'
    ORDER BY created_at DESC 
    LIMIT ${limit}
  `;
}

async function getContentMetrics(contentIds, tableName) {
  if (!contentIds.length) return {};

  const metrics = await sql`
    SELECT id, likes_count, comments_count, shares_count, hashtags
    FROM ${tableName}
    WHERE id = ANY(${contentIds})
  `;

  return metrics.reduce((acc, item) => {
    acc[item.id] = item;
    return acc;
  }, {});
}

function scoreContent(content, userContext, metrics) {
  const { mates, follows, interests, interactions, likedCreators } =
    userContext;
  const now = Date.now();
  const itemTime = new Date(content.created_at).getTime();
  let score = 0;

  score += content.strategyWeight || 0;

  if (mates.includes(content.user_id)) score += 50;
  if (follows.includes(content.user_id)) score += 30;
  if (likedCreators.includes(content.user_id)) score += 20;

  const itemMetrics = metrics[content.id] || {};
  score += (itemMetrics.likes_count || 0) * 2;
  score += (itemMetrics.comments_count || 0) * 3;
  score += (itemMetrics.shares_count || 0) * 4;

  const itemTags = itemMetrics.hashtags || [];
  const interestMatches = itemTags.filter((tag) =>
    interests.includes(tag)
  ).length;
  score += interestMatches * 15;

  if (interactions.hide?.includes(content.id)) {
    score -= 1000;
  }

  if (interactions.like?.includes(content.id)) {
    score += 10;
  }

  const hoursSinceCreation = (now - itemTime) / 3600000;
  const recency = Math.exp(-hoursSinceCreation / 48) * 100;
  score += recency;

  return Math.max(0, score);
}

async function generateTypedFeed(userId, tableName) {
  const userContext = await getUserContext(userId);
  const { mates, follows, interests, networkIds } = userContext;
  const trendingTags = await getTrendingTags(networkIds);
  const strategies = [];

  if (mates.length > 0) {
    strategies.push({ name: "mates", userIds: mates, weight: 50, limit: 15 });
  }

  if (follows.length > 0) {
    strategies.push({
      name: "follows",
      userIds: follows,
      weight: 30,
      limit: 15,
    });
  }

  if (interests.length > 0) {
    strategies.push({
      name: "interests",
      tags: interests,
      weight: 20,
      limit: 15,
    });
  }

  if (trendingTags.length > 0) {
    strategies.push({
      name: "trending",
      tags: trendingTags,
      weight: 15,
      limit: 15,
    });
  }

  if (userContext.likedCreators.length > 0) {
    strategies.push({
      name: "liked_creators",
      userIds: userContext.likedCreators.slice(0, 50),
      weight: 25,
      limit: 10,
    });
  }

  strategies.push({
    name: "popular",
    sortBy: "popular",
    weight: 15,
    limit: 15,
  });
  strategies.push({ name: "recent", sortBy: "recent", weight: 10, limit: 15 });

  const contentPromises = strategies.map((strategy) =>
    fetchContent(tableName, strategy, strategy.limit || 20).then((items) =>
      items.map((item) => ({
        ...item,
        strategyWeight: strategy.weight,
        strategyName: strategy.name,
      }))
    )
  );

  const contentResults = await Promise.all(contentPromises);
  const allContent = contentResults.flat();

  const contentMap = new Map();
  allContent.forEach((item) => {
    if (
      !contentMap.has(item.id) ||
      item.strategyWeight > contentMap.get(item.strategyWeight)
    ) {
      contentMap.set(item.id, item);
    }
  });

  const uniqueContent = Array.from(contentMap.values());
  const contentIds = uniqueContent.map((item) => item.id);
  const contentMetrics = await getContentMetrics(contentIds, tableName);

  const scoredContent = uniqueContent.map((item) => ({
    ...item,
    score: scoreContent(item, userContext, contentMetrics),
  }));

  return scoredContent
    .filter((item) => item.score > 0)
    .sort((a, b) => b.score - a.score)
    .slice(0, 20)
    .map((item) => item.id);
}

export async function precomputeFeed(userId, tableName) {
  if (tableName !== "posts" && tableName !== "clips") {
    throw new Error("Invalid content type. Must be 'posts' or 'clips'");
  }

  const cacheKey = `feed:${userId}:${tableName}`;
  const cacheTime = 7200;

  try {
    const cachedFeed = await rdb("get", cacheKey);
    const parsedFeed = cachedFeed ? JSON.parse(cachedFeed) : null;
    const cachedTimestamp = await rdb("get", `${cacheKey}:timestamp`);
    const now = Date.now();

    if (
      parsedFeed &&
      parsedFeed.length > 5 &&
      cachedTimestamp &&
      now - parseInt(cachedTimestamp) < cacheTime * 1000
    ) {
      return parsedFeed;
    }

    const contentIds = await generateTypedFeed(userId, tableName);

    await Promise.all([
      rdb("set", cacheKey, JSON.stringify(contentIds)),
      rdb("set", `${cacheKey}:timestamp`, now.toString()),
      rdb("expire", cacheKey, cacheTime),
      rdb("expire", `${cacheKey}:timestamp`, cacheTime),
    ]);

    return contentIds;
  } catch (error) {
    console.error(
      `Error precomputing ${tableName} feed for user ${userId}:`,
      error
    );
    throw error;
  }
}

export async function getStarterFeed(userId, tableName) {
  try {
    let contentIds;
    if (tableName === "posts") {
      const trendingPosts = await sql`
        SELECT id FROM posts
        ORDER BY created_at DESC
        LIMIT 20
      `;
      contentIds = trendingPosts.map((content) => content.id);
    } else if (tableName === "clips") {
      const trendingClips = await sql`
        SELECT id FROM clips
        ORDER BY created_at DESC
        LIMIT 20
      `;
      contentIds = trendingClips.map((content) => content.id);
    } else {
      throw new Error(`Invalid table name: ${tableName}`);
    }

    const cacheKey = `feed:${userId}:${tableName}`;
    const cacheTime = 7200;

    await Promise.all([
      rdb("set", cacheKey, JSON.stringify(contentIds)),
      rdb("expire", cacheKey, cacheTime),
    ]);

    return contentIds;
  } catch (error) {
    console.error(`Error in getStarterFeed:`, error);
    throw error;
  }
}

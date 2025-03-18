export const getFields = (req_fields) => {
  const validFields = new Set([
    "id",
    "user_id",
    "media_key",
    "collabs",
    "created_at",
  ]);

  const defaultFields =
    "c.id, c.user_id, c.media_key, c.collabs, c.created_at";

  if (!req_fields || !req_fields.length) return defaultFields;

  const safeFields = req_fields
    .filter((field) => validFields.has(field))
    .map((field) => `c.${field}`)
    .join(", ");

  return safeFields || defaultFields;
};
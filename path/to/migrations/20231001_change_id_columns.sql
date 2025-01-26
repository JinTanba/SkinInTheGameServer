ALTER TABLE "News"
  ALTER COLUMN id TYPE text USING id::text;

ALTER TABLE "TokenData"
  ALTER COLUMN id TYPE text USING id::text; 
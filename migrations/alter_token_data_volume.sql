ALTER TABLE "TokenData"
  ALTER COLUMN "volume" TYPE numeric
  USING "volume"::numeric; 
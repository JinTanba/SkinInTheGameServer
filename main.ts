/***************************************************
 * indexer.ts
 ***************************************************/
const { ethers, BigNumber } = require("ethers");
const { createClient, SupabaseClient } = require("@supabase/supabase-js");
const dotenv = require("dotenv");

// ABIファイル(ビルド済)のimport例
const factoryArtifact = require("./EtherFunFactory.json");
const saleArtifact = require("./EtherfunSale.json");

// .env 読み込み
dotenv.config();

// ======================= ENV & CONFIG =======================
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
const factoryAddress = process.env.FACTORY_ADDRESS;

if (!supabaseUrl || !supabaseServiceRoleKey || !factoryAddress) {
  throw new Error("Missing required environment variables.");
}

// Supabase クライアント
const supabase = createClient(supabaseUrl, supabaseServiceRoleKey);

// WebSocket RPCプロバイダ (Infura例: wss)
const provider = new ethers.providers.WebSocketProvider(
  "wss://arbitrum-sepolia.infura.io/ws/v3/63b354d57387411191e8c4819970577b"
);

/**
 * イベント引数の型定義例
 */
interface SaleCreatedEventArgs {
  saleContractAddress: string;
  creator: string;
  name: string;
  symbol: string;
  saleGoal: typeof BigNumber;
  logoUrl: string;
  description: string;
  // relatedLinksは無視
}

interface SaleLaunchedEventArgs {
  saleContractAddress: string;
  launcher: string;
}

interface TokensBoughtEventArgs {
  saleContractAddress: string;
  buyer: string;
  totalRaised: typeof BigNumber;
  tokenBalance: typeof BigNumber;
}

interface TokensSoldEventArgs {
  saleContractAddress: string;
  seller: string;
  totalRaised: typeof BigNumber;
  tokenBalance: typeof BigNumber;
}

interface MetaUpdatedEventArgs {
  saleContractAddress: string;
  logoUrl: string;
  description: string;
}

interface ClaimedEventArgs {
  saleContractAddress: string;
  claimant: string;
}

// ======================= テーブルスキーマ =======================

/**
 * Newsテーブル (idはDB側でauto increment, onchainAddressが一意)
 */
interface NewsSchema {
  id?: number;           // auto-increment PK
  onchainAddress: string; // コントラクトアドレス(文字列)
  date?: string;         // 日付を文字列で保存
  title?: string;
  description?: string;
  imageUrl?: string;
  isLaunched?: boolean;
}

/**
 * TokenDataテーブル (idはDB側でauto increment, onchainAddressが一意)
 */
interface VolumeHistoryItem {
  time: number;   // ms単位のtimestamp
  volume: number; // ETHでの合計調達額など
}

interface TokenDataSchema {
  id?: number;             // auto-increment PK
  onchainAddress: string;  // コントラクトアドレス(文字列)
  volume?: number;         
  volumeHistory?: VolumeHistoryItem[];
}

// ======================= テーブル名 =======================
const newsTableName = "News";
const tokenDataTableName = "TokenData";

// ======================= コントラクトインスタンス =======================
const factory = new ethers.Contract(factoryAddress, factoryArtifact, provider);

/**
 * ブロックからtimestamp(ms)を取得する
 * block.timestamp は秒単位なのでミリ秒に変換
 */
async function getBlockTimestampMs(event: Event): Promise<number> {
  return new Date().getTime();
}

/**
 * Newsテーブルに新規登録する（SaleCreated時）
 * - 既に同onchainAddressのレコードがあれば何もしない
 */
async function insertNewsOnSaleCreated(
  saleContractAddress: string,
  blockTimestampMs: number,
  name: string,
  description: string,
  logoUrl: string
): Promise<void> {
  const addressLower = saleContractAddress.toLowerCase();

  // すでにレコードが存在するかチェック
  const { data: existing, error: fetchError } = await supabase
    .from(newsTableName)
    .select("*")
    .eq("onchainAddress", addressLower)
    .maybeSingle();

  if (fetchError) {
    console.error("Error checking existing News record:", fetchError);
    return;
  }

  if (existing) {
    console.log(`[News] Already exists for onchainAddress=${addressLower}. Skip insertion.`);
    return;
  }

  // 存在しなければINSERT
  const { error: insertError } = await supabase
    .from(newsTableName)
    .insert([
      {
        onchainAddress: addressLower,
        date: String(blockTimestampMs),  // 文字列で保存 (必要に応じて toISOString() 等も可)
        title: name,
        description: description,
        imageUrl: logoUrl,
        isLaunched: false,
      }
    ]);

  if (insertError) {
    console.error("insertNewsOnSaleCreated error:", insertError);
  } else {
    console.log(`[News] Inserted new record for onchainAddress=${addressLower}`);
  }
}

/**
 * TokenDataテーブルに新規登録する (SaleCreated時)
 * - 既存があれば何もしない
 */
async function insertTokenDataOnSaleCreated(
  saleContractAddress: string
): Promise<void> {
  const addressLower = saleContractAddress.toLowerCase();

  // すでにレコードが存在するかチェック
  const { data: existing, error: fetchError } = await supabase
    .from(tokenDataTableName)
    .select("*")
    .eq("onchainAddress", addressLower)
    .maybeSingle();

  if (fetchError) {
    console.error("Error checking existing TokenData record:", fetchError);
    return;
  }

  if (existing) {
    console.log(`[TokenData] Already exists for onchainAddress=${addressLower}. Skip insertion.`);
    return;
  }

  // 存在しなければINSERT
  const { error: insertError } = await supabase
    .from(tokenDataTableName)
    .insert([
      {
        onchainAddress: addressLower,
        volume: 0,
        volumeHistory: [],
      }
    ]);

  if (insertError) {
    console.error("insertTokenDataOnSaleCreated error:", insertError);
  } else {
    console.log(`[TokenData] Inserted new record for onchainAddress=${addressLower}`);
  }
}

/**
 * SaleLaunchedイベント: Newsテーブルの isLaunched = true に更新
 */
async function updateNewsIsLaunched(saleContractAddress: string): Promise<void> {
  const addressLower = saleContractAddress.toLowerCase();

  const { error } = await supabase
    .from(newsTableName)
    .update({ isLaunched: true })
    .eq("onchainAddress", addressLower);

  if (error) {
    console.error("updateNewsIsLaunched error:", error);
  } else {
    console.log(`[News] isLaunched=true for onchainAddress=${addressLower}`);
  }
}

/**
 * TokensBought / TokensSoldイベント:
 * TokenDataテーブルの volume, volumeHistory を更新
 */
async function updateTokenDataVolume(
  saleContractAddress: string,
  totalRaised: typeof BigNumber,
  blockTimestampMs: number
): Promise<void> {
  try {
    const addressLower = saleContractAddress.toLowerCase();
    // wei -> Ether単位の number に変換
    const newVolumeEther = parseFloat(ethers.utils.formatEther(totalRaised));

    // 既存レコードを取得
    const { data: existing, error: fetchError } = await supabase
      .from(tokenDataTableName)
      .select("*")
      .eq("onchainAddress", addressLower)
      .maybeSingle();

    if (fetchError) {
      console.error("Supabase fetch error (TokenData):", fetchError);
      return;
    }

    if (!existing) {
      // まだ存在しない => 新規insert
      const volumeHistory: VolumeHistoryItem[] = [
        { time: blockTimestampMs, volume: newVolumeEther }
      ];
      const { error: insertError } = await supabase
        .from(tokenDataTableName)
        .insert([
          {
            onchainAddress: addressLower,
            volume: newVolumeEther,
            volumeHistory
          }
        ]);

      if (insertError) {
        console.error("Error inserting TokenData volume:", insertError);
      } else {
        console.log(`[TokenData] Inserted new row for onchainAddress=${addressLower}`);
      }
    } else {
      // 既存 => volumeHistoryに追記してupdate
      const oldHistory = existing.volumeHistory ?? [];
      oldHistory.push({ time: blockTimestampMs, volume: newVolumeEther });

      const { error: updateError } = await supabase
        .from(tokenDataTableName)
        .update({
          volume: newVolumeEther,
          volumeHistory: oldHistory
        })
        .eq("onchainAddress", addressLower);

      if (updateError) {
        console.error("Error updating TokenData volume:", updateError);
      } else {
        console.log(`[TokenData] Updated volume for onchainAddress=${addressLower}`);
      }
    }
  } catch (e) {
    console.error("updateTokenDataVolume error:", e);
  }
}

/**
 * MetaUpdatedイベント: Newsテーブルの imageUrl / description を更新
 */
async function updateNewsMetadata(
  saleContractAddress: string,
  logoUrl: string,
  description: string
): Promise<void> {
  const addressLower = saleContractAddress.toLowerCase();

  const { error } = await supabase
    .from(newsTableName)
    .update({
      imageUrl: logoUrl,
      description: description,
    })
    .eq("onchainAddress", addressLower);

  if (error) {
    console.error("updateNewsMetadata error:", error);
  } else {
    console.log(`[News] Updated metadata for onchainAddress=${addressLower}`);
  }
}

/**
 * Claimedイベント: 必要に応じてDBへ記録する、またはログのみ
 */
async function handleClaimed(
  saleContractAddress: string,
  claimant: string
): Promise<void> {
  console.log(`[Claimed] onchainAddress=${saleContractAddress}, claimant=${claimant}`);
  // 必要に応じて別テーブルに insert 等を実装
}

/**
 * 過去イベントの同期例
 */
async function syncPastEvents(fromBlock: number, toBlock: number): Promise<void> {
  console.log(`Syncing past events from block ${fromBlock} to block ${toBlock}...`);

  // ============== SaleCreated ==============
  const saleCreatedFilter = factory.filters.SaleCreated();
  const saleCreatedEvents = await factory.queryFilter(saleCreatedFilter, fromBlock, toBlock);
  for (const event of saleCreatedEvents) {
    const args = event.args as unknown as SaleCreatedEventArgs;
    if (!args) continue;
    const blockTimestampMs = await getBlockTimestampMs(event);

    await insertNewsOnSaleCreated(
      args.saleContractAddress,
      blockTimestampMs,
      args.name,
      args.description,
      args.logoUrl
    );

    await insertTokenDataOnSaleCreated(args.saleContractAddress);
  }

  // ============== SaleLaunched ==============
  const saleLaunchedFilter = factory.filters.SaleLaunched();
  const saleLaunchedEvents = await factory.queryFilter(saleLaunchedFilter, fromBlock, toBlock);
  for (const event of saleLaunchedEvents) {
    const args = event.args as unknown as SaleLaunchedEventArgs;
    if (!args) continue;

    await updateNewsIsLaunched(args.saleContractAddress);
  }

  // ============== TokensBought ==============
  const tokensBoughtFilter = factory.filters.TokensBought();
  const tokensBoughtEvents = await factory.queryFilter(tokensBoughtFilter, fromBlock, toBlock);
  for (const event of tokensBoughtEvents) {
    const args = event.args as unknown as TokensBoughtEventArgs;
    if (!args) continue;
    const blockTimestampMs = await getBlockTimestampMs(event);

    await updateTokenDataVolume(args.saleContractAddress, args.totalRaised, blockTimestampMs);
  }

  // ============== TokensSold ==============
  const tokensSoldFilter = factory.filters.TokensSold();
  const tokensSoldEvents = await factory.queryFilter(tokensSoldFilter, fromBlock, toBlock);
  for (const event of tokensSoldEvents) {
    const args = event.args as unknown as TokensSoldEventArgs;
    if (!args) continue;
    const blockTimestampMs = await getBlockTimestampMs(event);

    await updateTokenDataVolume(args.saleContractAddress, args.totalRaised, blockTimestampMs);
  }

  // ============== MetaUpdated ==============
  const metaUpdatedFilter = factory.filters.MetaUpdated();
  const metaUpdatedEvents = await factory.queryFilter(metaUpdatedFilter, fromBlock, toBlock);
  for (const event of metaUpdatedEvents) {
    const args = event.args as unknown as MetaUpdatedEventArgs;
    if (!args) continue;

    await updateNewsMetadata(args.saleContractAddress, args.logoUrl, args.description);
  }

  // ============== Claimed ==============
  const claimedFilter = factory.filters.Claimed();
  const claimedEvents = await factory.queryFilter(claimedFilter, fromBlock, toBlock);
  for (const event of claimedEvents) {
    const args = event.args as unknown as ClaimedEventArgs;
    if (!args) continue;

    await handleClaimed(args.saleContractAddress, args.claimant);
  }

  console.log("Sync done.");
}

/**
 * リアルタイムでイベントを購読
 */
function listenToEvents(): void {
  // SaleCreated
  factory.on(
    "SaleCreated",
    async (
      saleContractAddress: string,
      creator: string,
      name: string,
      symbol: string,
      saleGoal: typeof BigNumber,
      logoUrl: string,
      description: string,
      /* relatedLinks: string[], */
      event: Event
    ) => {
      console.log("[Event] SaleCreated:", saleContractAddress);
      const blockTimestampMs = await getBlockTimestampMs(event);

      await insertNewsOnSaleCreated(
        saleContractAddress,
        blockTimestampMs,
        name,
        description,
        logoUrl
      );
      await insertTokenDataOnSaleCreated(saleContractAddress);
    }
  );

  // SaleLaunched
  factory.on(
    "SaleLaunched",
    async (
      saleContractAddress: string,
      launcher: string,
      event: Event
    ) => {
      console.log("[Event] SaleLaunched:", saleContractAddress);
      await updateNewsIsLaunched(saleContractAddress);
    }
  );

  // TokensBought
  factory.on(
    "TokensBought",
    async (
      saleContractAddress: string,
      buyer: string,
      totalRaised: typeof BigNumber,
      tokenBalance: typeof BigNumber,
      event: Event
    ) => {
      console.log("[Event] TokensBought:", saleContractAddress, " buyer=", buyer);
      const blockTimestampMs = await getBlockTimestampMs(event);
      await updateTokenDataVolume(saleContractAddress, totalRaised, blockTimestampMs);
    }
  );

  // TokensSold
  factory.on(
    "TokensSold",
    async (
      saleContractAddress: string,
      seller: string,
      totalRaised: typeof BigNumber,
      tokenBalance: typeof BigNumber,
      event: Event
    ) => {
      console.log("[Event] TokensSold:", saleContractAddress, " seller=", seller);
      const blockTimestampMs = await getBlockTimestampMs(event);
      await updateTokenDataVolume(saleContractAddress, totalRaised, blockTimestampMs);
    }
  );

  // MetaUpdated
  factory.on(
    "MetaUpdated",
    async (
      saleContractAddress: string,
      logoUrl: string,
      description: string,
      event: Event
    ) => {
      console.log("[Event] MetaUpdated:", saleContractAddress);
      await updateNewsMetadata(saleContractAddress, logoUrl, description);
    }
  );

  // Claimed
  factory.on(
    "Claimed",
    async (
      saleContractAddress: string,
      claimant: string,
      event: Event
    ) => {
      console.log("[Event] Claimed:", saleContractAddress, " claimant=", claimant);
      await handleClaimed(saleContractAddress, claimant);
    }
  );
}

/**
 * トップレベルIIFEを使い、ファイル実行時に即監視を開始
 */
(async () => {
  try {
    console.log("=== Indexer Start ===");

    // 1. 過去ブロックのイベント同期 (例: 過去5万ブロック分)
    const latestBlock = await provider.getBlockNumber();
    const fromBlock = Math.max(latestBlock - 50000, 0);
    // await syncPastEvents(fromBlock, latestBlock);

    // 2. リアルタイム監視開始
    listenToEvents();

    console.log(`Listening for new events from block > ${latestBlock} ...`);
  } catch (err) {
    console.error(err);
    process.exit(1);
  }
})();

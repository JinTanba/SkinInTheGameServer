import { createClient } from "@supabase/supabase-js";
import { ethers } from "ethers";
import dotenv from "dotenv";
import { checkAndFixAllTokenData } from "./fixData";
import { replaceVolumeHistory } from "./replaceHistory";

dotenv.config();

// 処理回数をカウント
let processCount = 0;

// 環境変数
const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!supabaseUrl || !supabaseServiceRoleKey) {
  throw new Error("Missing env SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY.");
}

// Supabaseクライアント
const supabase = createClient(supabaseUrl, supabaseServiceRoleKey);

// テーブル名
const newsTableName = "News";
const tokenDataTableName = "TokenData";

// スキーマ
interface NewsSchema {
  id?: number;
  onchainAddress: string;
  title?: string;
}

interface VolumeHistoryItem {
  time: number;   // ms
  volume: number; // 
}

interface TokenDataSchema {
  id?: number;
  onchainAddress: string;
  volume?: number;              // numeric/double precision推奨
  volumeHistory?: VolumeHistoryItem[]; // jsonb
}

// 0.01 ~ 0.1 ETH の間でランダム
function getRandomEthAmount(): number {
  return Math.random() * (0.1 - 0.01) + 0.01;
}

/**
 * ランダム購入をシミュレーション (TokensBought 相当)
 */
async function updateTokenDataVolume(
  saleContractAddress: string,
  addedEthAmount: number
) {
  const addressLower = saleContractAddress.toLowerCase();

  // 既存のTokenDataを取得
  const { data: existing, error } = await supabase
    .from(tokenDataTableName)
    .select("*")
    .eq("onchainAddress", addressLower)
    .maybeSingle();

  if (error) {
    console.error("Error fetching TokenData:", error);
    return;
  }

  // 修正: volume が string で返る可能性があるため、parseFloat() で数値化
  const currentVolumeNum = existing && typeof existing.volume === "string"
    ? parseFloat(existing.volume)
    : existing?.volume ?? 0; 

  const newVolumeNum = currentVolumeNum + addedEthAmount;

  // JSON配列に追加
  const nowMs = Date.now();
  const oldHistory = existing?.volumeHistory ?? [];
  const newHistory = [
    ...oldHistory,
    {
      time: nowMs,
      volume: newVolumeNum,
    },
  ];

  if (!existing) {
    // 新規
    const { error: insertError } = await supabase
      .from(tokenDataTableName)
      .insert([
        {
          onchainAddress: addressLower,
          volume: newVolumeNum,
          volumeHistory: newHistory,
        },
      ]);
    if (insertError) {
      console.error("Error inserting TokenData:", insertError);
    } else {
      console.log(`[INSERT] volume=${newVolumeNum.toFixed(4)} for ${addressLower}`);
    }
  } else {
    // 既存
    const { error: updateError } = await supabase
      .from(tokenDataTableName)
      .update({
        volume: newVolumeNum,
        volumeHistory: newHistory,
      })
      .eq("onchainAddress", addressLower);
    if (updateError) {
      console.error("Error updating TokenData:", updateError);
    } else {
      console.log(`[UPDATE] volume=${newVolumeNum.toFixed(4)} for ${addressLower}`);
    }
  }
}

/**
 * ニュースを全部取得
 */
async function fetchAllNews(): Promise<NewsSchema[]> {
  const { data, error } = await supabase
    .from(newsTableName)
    .select("*");

  if (error) {
    console.error("Error fetching News:", error);
    return [];
  }
  return data ?? [];
}

/**
 * ニュースの中から1つランダムに選んで購入イベントをシミュレーション
 */
async function simulateRandomBuy(newsList: NewsSchema[]) {
  if (newsList.length === 0) return;
  const randomIndex = Math.floor(Math.random() * newsList.length);
  const selected = newsList[randomIndex];

  const buyAmount = getRandomEthAmount();
  console.log(`\n[simulateRandomBuy] +${buyAmount.toFixed(4)} ETH on ${selected.onchainAddress}`);
  await updateTokenDataVolume(selected.onchainAddress, buyAmount);

  // 処理回数をインクリメント
  processCount++;

  // 100回を超えた場合、データの修正と履歴の置き換えを実行
  if (processCount > 10000) {
    console.log("\n=== 100回の処理が完了しました。データの修正を開始します ===");
    
    // データの修正を実行
    await checkAndFixAllTokenData();
    
    // 履歴の置き換えを実行
    await replaceVolumeHistory();
    
    // カウントをリセット
    processCount = 0;
    
    console.log("=== データの修正が完了しました ===\n");
  }
}

(async () => {
  console.log("=== Demo: Every 1s random buy simulation ===");
  const allNews = await fetchAllNews();
  if (allNews.length === 0) {
    console.warn("No News found. Please insert some records in 'News' table first.");
    return;
  }

  // 1秒おきにランダム購入
  setInterval(() => {
    simulateRandomBuy(allNews).catch((err) => {
      console.error("simulateRandomBuy error:", err);
    });
  }, 1000);

  console.log("Running... (Ctrl+C to stop)");
})();

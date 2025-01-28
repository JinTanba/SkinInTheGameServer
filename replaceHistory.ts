import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";

dotenv.config();

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

if (!supabaseUrl || !supabaseServiceRoleKey) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
}

// Supabase クライアント
const supabase = createClient(supabaseUrl, supabaseServiceRoleKey);

// テーブル名
const newsTableName = "News";
const tokenDataTableName = "TokenData";

// スキーマ例
interface NewsSchema {
  id?: number;
  onchainAddress: string;
  title?: string;
  // ...省略
}

interface VolumeHistoryItem {
  time: number;   // ms単位
  volume: number; // ETHでの値
}

interface TokenDataSchema {
  id?: number;
  onchainAddress: string;
  volume?: number;  
  volumeHistory?: VolumeHistoryItem[];
}

/**
 * ランダムな「乱高下」データを生成する
 * 例:
 * - length = 10 ステップ
 * - 最初の値は 0 ~ 0.5 のランダム
 * - 各ステップで -0.1 ~ +0.2 の変動
 * - 下限は 0, 上限は任意(例: 5 ETHとか) …ここではあまり上限は気にしない
 * - 時間は現在時刻を起点に 10分刻み (600,000 ms)
 */
function generateRandomVolHistory(length: number): VolumeHistoryItem[] {
  const now = Date.now();
  const result: VolumeHistoryItem[] = [];

  // 初期ボリューム 0 ~ 0.5
  let currentVol = Math.random() * 0.5;

  for (let i = 0; i < length; i++) {
    // i ステップ目の時刻 = now - (length - i)*10分
    // => 過去から現在に向けての時系列を作るならこう。逆に未来方向でもOK。
    // ここでは「古い時刻 -> 新しい時刻」の順で作りたいので:
    const t = now - (length - i) * 600_000;

    // push
    result.push({
      time: t,
      volume: parseFloat(currentVol.toFixed(4)), // 小数4桁に丸める
    });

    // 次ステップで変動: -0.1 ~ +0.2
    const delta = Math.random() * 0.3 - 0.1; // (0.3 => max 0.2 above, -0.1 below)
    currentVol += delta;
    if (currentVol < 0) {
      currentVol = 0;
    }
  }

  // 時間昇順に並んでいるか確認(今のロジックだと既に昇順)
  return result;
}

/**
 * Newsテーブルを全件取得
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
 * onchainAddress ごとに TokenData.volumeHistory をランダムに置き換える
 */
async function replaceVolumeHistoryForAddress(onchainAddress: string) {
  // 1) 既存 TokenData を取得
  const lowerAddr = onchainAddress.toLowerCase();
  const { data: existing, error } = await supabase
    .from(tokenDataTableName)
    .select("*")
    .eq("onchainAddress", lowerAddr)
    .maybeSingle();

  if (error) {
    console.error("Error fetching TokenData:", error);
    return;
  }

  // 2) 新たな volumeHistory (例: 10ステップの乱高下) を生成
  const newHistory = generateRandomVolHistory(10);
  // 最後のvolumeを "volume" カラムに設定
  const finalVolume = newHistory[newHistory.length - 1].volume;

  // 3) まだTokenData無ければINSERT, あればUPDATE
  if (!existing) {
    // INSERT
    const { error: insertError } = await supabase
      .from(tokenDataTableName)
      .insert([
        {
          onchainAddress: lowerAddr,
          volume: finalVolume,
          volumeHistory: newHistory,
        },
      ]);
    if (insertError) {
      console.error("Insert error for onchainAddress=", lowerAddr, insertError);
    } else {
      console.log(`[INSERT] Replaced volumeHistory for ${lowerAddr}, finalVolume=${finalVolume}`);
    }
  } else {
    // UPDATE
    const { error: updateError } = await supabase
      .from(tokenDataTableName)
      .update({
        volume: finalVolume,
        volumeHistory: newHistory,
      })
      .eq("onchainAddress", lowerAddr);

    if (updateError) {
      console.error("Update error for onchainAddress=", lowerAddr, updateError);
    } else {
      console.log(`[UPDATE] Replaced volumeHistory for ${lowerAddr}, finalVolume=${finalVolume}`);
    }
  }
}

/**
 * メインスクリプト:
 * 1. Newsを一覧取得
 * 2. 各News.onchainAddressの volumeHistory を「乱高下データ」に置き換える
 */
export async function replaceVolumeHistory() {
  try {
    console.log("=== replaceVolumeHistory script start ===");
    const allNews = await fetchAllNews();
    if (allNews.length === 0) {
      console.log("No News found. Please insert some rows in News first.");
      process.exit(0);
    }

    // 全Newsをループ
    for (const news of allNews) {
      // TokenDataを replace
      await replaceVolumeHistoryForAddress(news.onchainAddress);
    }

    console.log("=== Done replaceVolumeHistory ===");
  } catch (err) {
    console.error("Script error:", err);
    process.exit(1);
  }
}

import { createClient } from "@supabase/supabase-js";
import dotenv from "dotenv";

// .envから環境変数を読み込む
dotenv.config();

const supabaseUrl = process.env.SUPABASE_URL;
const supabaseServiceRoleKey = process.env.SUPABASE_SERVICE_ROLE_KEY;
if (!supabaseUrl || !supabaseServiceRoleKey) {
  throw new Error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env");
}

// Supabaseクライアント
const supabase = createClient(supabaseUrl, supabaseServiceRoleKey);

// テーブル名
const tokenDataTableName = "TokenData";

interface VolumeHistoryItem {
  time: number;
  volume: number;
}

interface TokenDataSchema {
  id?: number;
  onchainAddress: string;
  volume?: number;
  volumeHistory?: VolumeHistoryItem[]; // 実際には JSONB
}

/**
 * volumeHistory配列を点検し、以下を実施:
 *  1) "0.0117802051273427440.02837845831182257" のように
 *     「ドットが2つ以上ある」or「数値変換できない」データは削除する
 *  2) 最新の4件だけ残す (time昇順でソートし、先頭から削る形)
 *  3) 正常にパースできたものは { time, volume } を数値型として保持
 */
function fixVolumeHistory(original: any[]): VolumeHistoryItem[] {
  if (!original || !Array.isArray(original)) return [];

  // 1) まず配列を複製 (破壊的変更を避ける)
  const cloned = [...original];

  // 2) それぞれをチェックし、「不正な volume」は除外
  //    - volume が文字列で '.' が2つ以上含まれる
  //    - parseFloat() が NaN になる
  //    - time が数値に変換できない
  const filtered = cloned.filter((item) => {
    if (!item || item.volume == null) {
      return false;
    }

    let v: number;
    if (typeof item.volume === "number") {
      v = item.volume;
    } else if (typeof item.volume === "string") {
      // ドットが2つ以上含まれているかチェック
      const dotsCount = (item.volume.match(/\./g) || []).length;
      if (dotsCount > 1) {
        // 「0.01178...0.02837...」のようにドット2つ以上ある => 不正
        return false;
      }
      const parsed = parseFloat(item.volume);
      if (isNaN(parsed)) {
        return false;
      }
      v = parsed;
    } else {
      return false;
    }

    // time も一応数値化 (NaNなら排除)
    let t: number;
    if (typeof item.time === "number") {
      t = item.time;
    } else {
      t = parseFloat(String(item.time));
      if (isNaN(t)) {
        return false;
      }
    }

    // ここまで来たらOK -> あとで再構築
    return true;
  });

  // 3) timeでソート (昇順)
  const sorted = filtered.sort((a, b) => {
    const tA = typeof a.time === "number" ? a.time : parseFloat(a.time);
    const tB = typeof b.time === "number" ? b.time : parseFloat(b.time);
    return tA - tB;
  });

  // 4) 最新の4つだけ残す => 要素数が多い場合は「古いほう」から削除
  //    ソート済なので、後ろから4件残す
  const startIndex = sorted.length > 4 ? sorted.length - 7 : 0;
  const truncated = sorted.slice(startIndex);

  // 5) { time, volume } を数値型で再構築
  const final = truncated.map((item) => ({
    time: typeof item.time === "number" ? item.time : parseFloat(item.time),
    volume: typeof item.volume === "number" ? item.volume : parseFloat(item.volume)
  }));

  return final;
}

/**
 * TokenData の全レコードについて volumeHistory を点検・修正
 * - 不正な数値を含むエントリを削除
 * - 4件を超える分は古い方から削除し最新4件だけ残す
 * - 変更があった場合、volumeHistory とあわせて volume も最終値に更新
 */
export async function checkAndFixAllTokenData() {
  // 1) 全件取得
  const { data: rows, error } = await supabase
    .from(tokenDataTableName)
    .select("*");

  if (error) {
    console.error("Error fetching TokenData:", error);
    return;
  }
  if (!rows || rows.length === 0) {
    console.log("No TokenData found.");
    return;
  }

  for (const row of rows) {
    try {
      const originalHistory = row.volumeHistory ?? [];
      // 修正前後を比較するために JSON.stringify
      const originalStr = JSON.stringify(originalHistory);

      console.log("-------------------",originalHistory);

      // 2) 修正処理
      const fixed = fixVolumeHistory(originalHistory);
      const fixedStr = JSON.stringify(fixed);

      // 3) 差分がない場合は何もしない
      if (originalStr === fixedStr) {
        console.log(`[OK] onchainAddress=${row.onchainAddress} => no changes`);
        continue;
      }

      // 4) 最終の volume を更新 (fixed が空なら0)
      let newVolume = 0;
      if (fixed.length > 0) {
        newVolume = fixed[fixed.length - 1].volume;
      }

      console.log("-------------------",fixed);

      // 5) Supabase に UPDATE
      const { error: updateError } = await supabase
        .from(tokenDataTableName)
        .update({
          volumeHistory: fixed, // 最新の4つ＆不正データ排除後の配列
          volume: newVolume     // 最終 volumeHistory の値にあわせる
        })
        .eq("onchainAddress", row.onchainAddress);

      if (updateError) {
        console.error(`[UPDATE ERROR] onchainAddress=${row.onchainAddress}`, updateError);
      } else {
        console.log(`[FIXED] onchainAddress=${row.onchainAddress} => final volume=${newVolume}, history length=${fixed.length}`);
      }
    } catch (err) {
      console.error(`[ERROR in row onchainAddress=${row.onchainAddress}]`, err);
    }
  }
}


